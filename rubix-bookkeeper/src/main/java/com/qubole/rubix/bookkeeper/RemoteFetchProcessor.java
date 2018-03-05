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
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class RemoteFetchProcessor extends AbstractScheduledService
{
  private Configuration conf;
  BookKeeper bookKeeper;
  private Queue<FetchRequest> processQueue = null;
  private Runnable runnableStatusUpdateTask = null;
  private ExecutorService processService;

  private static DirectBufferPool bufferPool = new DirectBufferPool();

  int diskReadBufferSize;
  int numRemoteFetchThreads;
  int remoteFecthThreadInitialDelay;
  int remoteFetchThreadInterval;
  int processThreadInitalDelay;
  int processThreadInterval;
  long requestProcessDelay;

  private static final Log log = LogFactory.getLog(RemoteFetchProcessor.class);

  public RemoteFetchProcessor(BookKeeper bookKeeper, Configuration conf)
  {
    this.bookKeeper = bookKeeper;
    this.conf = conf;
    this.processQueue = new ConcurrentLinkedQueue<FetchRequest>();

    this.diskReadBufferSize = CacheConfig.getDiskReadBufferSizeDefault(conf);
    this.processThreadInitalDelay = CacheConfig.getProcessThreadInitialDelayInMs(conf);
    this.processThreadInterval = CacheConfig.getProcessThreadIntervalInMs(conf);
    this.requestProcessDelay = CacheConfig.getRemoteFetchProcessIntervalInMS(conf);
    int numThreads = CacheConfig.getRemoteFetchNumThreads(conf);

    ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(numThreads);
    processService = MoreExecutors.getExitingExecutorService(executor);
  }

  public void addToProcessQueue(String remotePath, long offset, int length, long fileSize, long lastModified)
  {
    long requestedTime = System.currentTimeMillis();
    FetchRequest request = new FetchRequest(remotePath, offset, length, fileSize, lastModified, requestedTime);
    processQueue.add(request);
  }

  @Override
  protected void runOneIteration() throws Exception
  {
    long currentTime = System.currentTimeMillis();

    processRequest(currentTime);
  }

  private void processRequest(long currentTime) throws IOException, InterruptedException, ExecutionException
  {
    ConcurrentMap<String, FileMetadataRequest> fetchRequestMap = new ConcurrentHashMap<String, FileMetadataRequest>();
    ConcurrentMap<String, RangeSet<Long>> rangeMap = new ConcurrentHashMap<String, RangeSet<Long>>();

    // Till the queue is not empty or there are no more requests which came in before the configured delay time
    // we are going to collect the requests and process them
    while (!processQueue.isEmpty()) {
      FetchRequest request = processQueue.peek();
      if (currentTime - request.getRequestedTime() < this.requestProcessDelay) {
        break;
      }

      if (!rangeMap.containsKey(request.getRemotePath())) {
        RangeSet<Long> rangeSet = TreeRangeSet.create();
        rangeMap.putIfAbsent(request.getRemotePath(), rangeSet);
        fetchRequestMap.putIfAbsent(request.getRemotePath(), new FileMetadataRequest(request.getRemotePath(),
            request.getFileSize(), request.getLastModified()));
      }
      rangeMap.get(request.getRemotePath()).add(Range.open(request.getOffset(),
          request.getOffset() + request.getLength()));
      processQueue.remove();
    }

    processRemoteFetchRequest(fetchRequestMap, rangeMap);

    // After every iteration we are clearing the maps
    rangeMap.clear();
    fetchRequestMap.clear();
  }

  @Override
  protected Scheduler scheduler()
  {
    return Scheduler.newFixedDelaySchedule(processThreadInitalDelay, processThreadInterval, TimeUnit.MILLISECONDS);
  }

  private void processRemoteFetchRequest(ConcurrentMap<String, FileMetadataRequest> fetchRequestMap,
                                         ConcurrentMap<String, RangeSet<Long>> rangeMap)
                                        throws IOException, InterruptedException, ExecutionException
  {
    final List<FileDownloadRequestChain> readRequestChainList = new ArrayList<FileDownloadRequestChain>();

    for (Iterator<Map.Entry<String, RangeSet<Long>>> it = rangeMap.entrySet().iterator(); it.hasNext(); ) {
      Map.Entry<String, RangeSet<Long>> entry = it.next();
      Path path = new Path(entry.getKey());
      FileSystem fs = path.getFileSystem(conf);
      fs.initialize(path.toUri(), conf);
      String localPath = CacheConfig.getLocalPath(entry.getKey(), conf);
      log.info("Processing Request for File : " + path.toString() + " LocalFile : " + localPath);
      FileMetadataRequest fileRequestMetadata = fetchRequestMap.get(entry.getKey());
      ByteBuffer directWriteBuffer = bufferPool.getBuffer(diskReadBufferSize);

      FileDownloadRequestChain requestChain = new FileDownloadRequestChain(fs, localPath,
          directWriteBuffer, conf, fileRequestMetadata.remotePath, fileRequestMetadata.fileSize,
          fileRequestMetadata.lastModified);

      for (Range<Long> range : entry.getValue().asRanges()) {
        log.info("Adding request for File : " + entry.getKey() + " Start : "
            + range.lowerEndpoint() + " End : " + range.upperEndpoint());
        ReadRequest request = new ReadRequest(range.lowerEndpoint(), range.upperEndpoint(),
            range.lowerEndpoint(), range.upperEndpoint(), null, 0, fetchRequestMap.get(entry.getKey()).fileSize);
        requestChain.addReadRequest(request);
      }
      it.remove();

      log.info("Request added for file: " + requestChain.getRemotePath() + " Number of Requests : " +
              requestChain.getReadRequests().size());
      readRequestChainList.add(requestChain);
    }

    processDownloadRequests(readRequestChainList);
  }

  private void processDownloadRequests(List<FileDownloadRequestChain> readRequestChainList)
  {
    int sizeRead = 0;
    List<Future<Integer>> futures = new ArrayList<Future<Integer>>();

    for (FileDownloadRequestChain requestChain : readRequestChainList) {
      requestChain.lock();
      Future<Integer> result = processService.submit(requestChain);
      futures.add(result);
    }

    for (Future<Integer> future : futures) {
      FileDownloadRequestChain requestChain = readRequestChainList.get(futures.indexOf(future));
      try {
        int read = future.get();
        sizeRead += read;
        requestChain.updateCacheStatus(requestChain.getRemotePath(), requestChain.getFileSize(),
            requestChain.getLastModified(), CacheConfig.getBlockSize(conf), conf);
      }
      catch (ExecutionException | InterruptedException ex) {
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
