/**
 * Copyright (c) 2016. Qubole Inc
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */
package com.qubole.rubix.bookkeeper;

import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import com.google.common.util.concurrent.AbstractScheduledService;
import com.google.common.util.concurrent.MoreExecutors;
import com.qubole.rubix.core.ReadRequest;
import com.qubole.rubix.core.FileDownloadRequestChain;
import com.qubole.rubix.spi.CacheConfig;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.DirectBufferPool;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ExecutionException;

public class RemoteFetchProcessor extends AbstractScheduledService
{
  private Configuration conf;
  BookKeeper bookKeeper;
  private Queue<FetchRequest> processQueue = null;
  private ConcurrentMap<String, RangeSet<Long>> rangeMap = null;
  private ConcurrentMap<String, FileMetadataRequest> fetchRequestMap = null;
  private static ScheduledExecutorService processService = MoreExecutors.getExitingScheduledExecutorService(
      new ScheduledThreadPoolExecutor(10));

  Runnable runnableStatusUpdateTask = null;
  int diskReadBufferSize;
  private static DirectBufferPool bufferPool = new DirectBufferPool();

  private static final Log log = LogFactory.getLog(RemoteFetchProcessor.class);

  public RemoteFetchProcessor(BookKeeper bookKeeper, Configuration conf)
  {
    this.bookKeeper = bookKeeper;
    this.conf = conf;
    this.processQueue = new ConcurrentLinkedQueue<FetchRequest>();
    this.rangeMap = new ConcurrentHashMap<String, RangeSet<Long>>();
    this.fetchRequestMap = new ConcurrentHashMap<String, FileMetadataRequest>();
    this.diskReadBufferSize = CacheConfig.getDiskReadBufferSizeDefault(conf);

    runnableStatusUpdateTask = new Runnable()
    {
      @Override
      public void run()
      {
        try {
          processFetchRequest();
        }
        catch (Exception ex) {
          log.error(ex);
        }
      }
    };
    processService.scheduleWithFixedDelay(runnableStatusUpdateTask, 1000, 100, TimeUnit.MILLISECONDS);
  }

  public void addToProcessQueue(String remotePath, long offset, int length, long fileSize, long lastModified)
  {
    FetchRequest request = new FetchRequest(remotePath, offset, length, fileSize, lastModified);
    processQueue.add(request);
  }

  @Override
  protected void runOneIteration() throws Exception
  {
    if (!processQueue.isEmpty()) {
      FetchRequest request = processQueue.remove();
      log.info("Processing Request : RemotePath - " + request.getRemotePath() + " Offset : " +
              request.getOffset() + " Length : " + request.getLength());

      if (!rangeMap.containsKey(request.getRemotePath())) {
        RangeSet<Long> rangeSet = TreeRangeSet.create();
        rangeMap.putIfAbsent(request.getRemotePath(), rangeSet);
        fetchRequestMap.putIfAbsent(request.getRemotePath(), new FileMetadataRequest(request.getRemotePath(),
            request.getFileSize(), request.getLastModified()));
      }

      String remotePath = request.getRemotePath();
      synchronized (remotePath) {
        RangeSet<Long> rangeSet = rangeMap.get(remotePath);
        rangeSet.add(Range.open(request.getOffset(), request.getOffset() + request.getLength()));
        log.info("RangeSet : " + rangeSet);
      }
    }
  }

  @Override
  protected Scheduler scheduler()
  {
    return Scheduler.newFixedRateSchedule(1000, 10, TimeUnit.MILLISECONDS);
  }

  private void processFetchRequest() throws IOException, InterruptedException, ExecutionException
  {
    final List<FileDownloadRequestChain> readRequestChainList = new ArrayList<FileDownloadRequestChain>();

    for (Iterator<Map.Entry<String, RangeSet<Long>>> it = rangeMap.entrySet().iterator(); it.hasNext(); ) {
      Map.Entry<String, RangeSet<Long>> entry = it.next();
      Path path = new Path(entry.getKey());
      FileSystem fs = path.getFileSystem(conf);
      fs.initialize(path.toUri(), conf);
      FSDataInputStream inputStream = fs.open(path);
      String localPath = CacheConfig.getLocalPath(entry.getKey(), conf);
      log.info("Processing Request for File : " + path.toString() + " LocalFile : " + localPath);
      FileMetadataRequest fileRequestMetadata = fetchRequestMap.get(entry.getKey());
      ByteBuffer directWriteBuffer = bufferPool.getBuffer(diskReadBufferSize);

      FileDownloadRequestChain requestChain = new FileDownloadRequestChain(fs, localPath,
          directWriteBuffer, conf, fileRequestMetadata.remotePath, fileRequestMetadata.fileSize,
          fileRequestMetadata.lastModified);

      synchronized (entry.getKey()) {
        for (Range<Long> range : entry.getValue().asRanges()) {
          log.info("Adding request for File : " + entry.getKey() + " Start : "
              + range.upperEndpoint() + " End : " + range.lowerEndpoint());
          ReadRequest request = new ReadRequest(range.lowerEndpoint(), range.upperEndpoint(),
              range.lowerEndpoint(), range.upperEndpoint(), null, 0, fetchRequestMap.get(entry.getKey()).fileSize);
          requestChain.addReadRequest(request);
        }
        it.remove();
      }
      log.info("Request added for file: " + requestChain.getRemotePath() + " Number of Requests : " +
              requestChain.getReadRequests().size());
      readRequestChainList.add(requestChain);
    }

    int sizeRead = 0;
    for (FileDownloadRequestChain requestChain : readRequestChainList) {
      try {
        requestChain.lock();
        sizeRead += requestChain.call();
        requestChain.updateCacheStatus(requestChain.getRemotePath(), requestChain.getFileSize(),
                requestChain.getLastModified(), CacheConfig.getBlockSize(conf), conf);
      }
      catch (IOException ex) {
        log.error(ex.getStackTrace());
        requestChain.cancel();
      }
    }

  }

  private class FileMetadataRequest
  {
    private String remotePath;
    private long fileSize;
    private long lastModified;

    FileMetadataRequest(String remotePath, long fileSize, long lastModified)
    {
      this.remotePath = remotePath;
      this.fileSize = fileSize;
      this.lastModified = lastModified;
    }
  }
}
