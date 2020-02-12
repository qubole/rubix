/**
 * Copyright (c) 2019. Qubole Inc
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

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.Range;
import com.google.common.util.concurrent.MoreExecutors;
import com.qubole.rubix.bookkeeper.utils.DiskUtils;
import com.qubole.rubix.common.metrics.BookKeeperMetrics;
import com.qubole.rubix.core.ReadRequest;
import com.qubole.rubix.spi.CacheConfig;
import com.qubole.rubix.spi.CacheUtil;
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
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Created by Abhishek on 3/9/18.
 */
class FileDownloader
{
  Configuration conf;
  private ExecutorService processService;
  int diskReadBufferSize;
  private MetricRegistry metrics;
  private Counter totalMBDownloaded;
  private Counter totalTimeToDownload;
  BookKeeper bookKeeper;

  private static final Log log = LogFactory.getLog(FileDownloader.class);
  private static DirectBufferPool bufferPool = new DirectBufferPool();

  public FileDownloader(BookKeeper bookKeeper, MetricRegistry metrics, Configuration conf)
  {
    this.bookKeeper = bookKeeper;
    this.conf = conf;
    this.metrics = metrics;
    int numThreads = CacheConfig.getRemoteFetchThreads(conf);
    this.diskReadBufferSize = CacheConfig.getDiskReadBufferSize(conf);

    ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(numThreads);
    processService = MoreExecutors.getExitingExecutorService(executor);

    initializeMetrics();
  }

  private void initializeMetrics()
  {
    totalMBDownloaded = metrics.counter(BookKeeperMetrics.CacheMetric.ASYNC_DOWNLOADED_MB_COUNT.getMetricName());
    totalTimeToDownload = metrics.counter(BookKeeperMetrics.CacheMetric.ASYNC_DOWNLOAD_TIME_COUNT.getMetricName());
  }

  protected List<FileDownloadRequestChain> getFileDownloadRequestChains(ConcurrentMap<String, DownloadRequestContext> contextMap)
      throws IOException
  {
    List<FileDownloadRequestChain> readRequestChainList = new ArrayList<FileDownloadRequestChain>();
    for (Map.Entry<String, DownloadRequestContext> entry : contextMap.entrySet()) {
      Path path = new Path(entry.getKey());
      DownloadRequestContext context = entry.getValue();

      // Creating a new instance of the filesystem object by calling FileSystem.newInstance
      // This one makes sure we will get a new instance even if fs.%.impl.disable.cache is set to false
      FileSystem fs = FileSystem.get(path.toUri(), conf);
      fs.initialize(path.toUri(), conf);

      String localPath = CacheUtil.getLocalPath(entry.getKey(), conf);
      log.debug("Processing Request for File : " + path.toString() + " LocalFile : " + localPath);
      ByteBuffer directWriteBuffer = bufferPool.getBuffer(diskReadBufferSize);

      FileDownloadRequestChain requestChain = new FileDownloadRequestChain(bookKeeper, fs, localPath,
          directWriteBuffer, conf, context.getRemoteFilePath(), context.getFileSize(),
          context.getLastModifiedTime());

      for (Range<Long> range : context.getRanges().asRanges()) {
        ReadRequest request = new ReadRequest(range.lowerEndpoint(), range.upperEndpoint(),
            range.lowerEndpoint(), range.upperEndpoint(), null, 0, context.getFileSize());
        requestChain.addReadRequest(request);
      }

      log.debug("Request added for file: " + requestChain.getRemotePath() + " Number of Requests : " +
          requestChain.getReadRequests().size());

      readRequestChainList.add(requestChain);
    }

    return readRequestChainList;
  }

  protected long processDownloadRequests(List<FileDownloadRequestChain> readRequestChainList)
  {
    if (readRequestChainList.size() == 0) {
      return 0;
    }

    long sizeRead = 0;
    List<Future<Integer>> futures = new ArrayList<Future<Integer>>();

    for (FileDownloadRequestChain requestChain : readRequestChainList) {
      requestChain.lock();
      Future<Integer> result = processService.submit(requestChain);
      futures.add(result);
    }

    for (Future<Integer> future : futures) {
      FileDownloadRequestChain requestChain = readRequestChainList.get(futures.indexOf(future));
      long totalBytesToBeDownloaded = 0;
      for (ReadRequest request : requestChain.getReadRequests()) {
        totalBytesToBeDownloaded += request.getBackendReadLength();
      }
      try {
        long read = future.get();
        // Updating the cache only when we have downloaded the same amount of data that we requested
        // This takes care of the scenario where the data is download partially but the cache
        // metadata gets updated for all the requested blocks.
        if (read == totalBytesToBeDownloaded) {
          requestChain.updateCacheStatus(requestChain.getRemotePath(), requestChain.getFileSize(),
              requestChain.getLastModified(), CacheConfig.getBlockSize(conf), conf);
          sizeRead += read;
          this.totalTimeToDownload.inc(requestChain.getTimeSpentOnDownload());
        }
        else {
          log.error("ReadData didn't match with requested value. RequestedData: " + totalBytesToBeDownloaded +
              " ReadData: " + read);
        }
      }
      catch (ExecutionException | InterruptedException ex) {
        log.error(ex);
        requestChain.cancel();
      }
    }
    long dataDownloadedInMB = DiskUtils.bytesToMB(sizeRead);
    this.totalMBDownloaded.inc(dataDownloadedInMB);
    return sizeRead;
  }
}
