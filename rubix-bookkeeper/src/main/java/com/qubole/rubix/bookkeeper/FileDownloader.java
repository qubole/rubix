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
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.qubole.rubix.common.metrics.BookKeeperMetrics;
import com.qubole.rubix.core.ReadRequest;
import com.qubole.rubix.spi.CacheConfig;
import com.qubole.rubix.spi.CacheUtil;
import com.qubole.rubix.spi.thrift.BlockLocation;
import com.qubole.rubix.spi.thrift.CacheStatusRequest;
import com.qubole.rubix.spi.thrift.CacheStatusResponse;
import com.qubole.rubix.spi.thrift.Location;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.DirectBufferPool;

import java.io.FileNotFoundException;
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

import static com.qubole.rubix.spi.utils.DataSizeUnits.BYTES;
import static com.qubole.rubix.spi.CacheUtil.UNKONWN_GENERATION_NUMBER;
import static com.qubole.rubix.spi.CommonUtilities.toBlockStartPosition;
import static com.qubole.rubix.spi.CommonUtilities.toEndBlock;
import static com.qubole.rubix.spi.CommonUtilities.toStartBlock;

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

  private final RemoteFetchProcessor remoteFetchProcessor;
  private final BookKeeper bookKeeper;

  private static final Log log = LogFactory.getLog(FileDownloader.class);
  private static DirectBufferPool bufferPool = new DirectBufferPool();

  public FileDownloader(BookKeeper bookKeeper, MetricRegistry metrics, Configuration conf, RemoteFetchProcessor remoteFetchProcessor)
  {
    this.bookKeeper = bookKeeper;
    this.remoteFetchProcessor = remoteFetchProcessor;
    this.conf = conf;
    this.metrics = metrics;
    int numThreads = CacheConfig.getRemoteFetchThreads(conf);
    this.diskReadBufferSize = CacheConfig.getDiskReadBufferSize(conf);

    ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(numThreads, new ThreadFactoryBuilder()
            .setNameFormat("parallel-warmup-%s")
            .setDaemon(true)
            .build());
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
      int generationNumber = UNKONWN_GENERATION_NUMBER;

      FileSystem fs = FileSystem.get(path.toUri(), conf);
      fs.initialize(path.toUri(), conf);

      ByteBuffer directWriteBuffer = bufferPool.getBuffer(diskReadBufferSize);
      FileDownloadRequestChain requestChain = null;
      String remotePath = entry.getKey();

      Range<Long> previousRange = null;
      for (Range<Long> range : context.getRanges().asRanges()) {
        // align range to block boundary
        long startBlock = toStartBlock(range.lowerEndpoint(), conf);
        long endBlock = toEndBlock(range.upperEndpoint(), conf);

        // We can get cases where multiple reads are part of same Block
        Range<Long> currentRange = Range.closedOpen(startBlock, endBlock);
        if (previousRange != null && previousRange.encloses(currentRange)) {
          // already covered in previous request
          continue;
        }

        previousRange = currentRange;

        // Avoid duplicate warm-ups
        List<BlockLocation> blockLocations = null;

        try {
          CacheStatusResponse response = bookKeeper.getCacheStatus(
                  new CacheStatusRequest(
                          context.getRemoteFilePath(),
                          context.getFileSize(),
                          context.getLastModifiedTime(),
                          startBlock,
                          endBlock));
          blockLocations = response.getBlocks();
          if(generationNumber != UNKONWN_GENERATION_NUMBER && response.getGenerationNumber() != generationNumber) {
            log.debug(String.format("Mismatch in generation-number in download requests for file %s, expected=%d but found=%d, skipping the file",
                    remotePath, generationNumber, response.getGenerationNumber()));
            // Do not add the requests back as there has been invalidation of the file which means there is a good chance
            // that the file is not needed anymore. If it is needed, then next read will add new requests for it
            requestChain = null;
            break;
          }
          generationNumber = response.getGenerationNumber();
          if (requestChain == null)
          {
            String localPath = CacheUtil.getLocalPath(remotePath, conf, generationNumber);
            log.debug("Processing Request for File : " + path.toString() + " LocalFile : " + localPath);
            requestChain = new FileDownloadRequestChain(bookKeeper, fs, localPath,
                    directWriteBuffer, conf, context.getRemoteFilePath(), context.getFileSize(),
                    context.getLastModifiedTime(), generationNumber);
          }
        }
        catch (Exception e) {
          log.warn("Error communicating with bookKeeper", e);
          // Exception is not expected as RemoteFetchProcessor ensures to not start processing until BookKeeper has initialized
          // recover from this, requeue the requests for this file and continue with next file
          remoteFetchProcessor.addToProcessQueueSafe(context.getRemoteFilePath(), context.getRanges().asRanges(), context.getFileSize(), context.getLastModifiedTime());
          requestChain = null;
          break;
        }

        for (int i = 0; i < blockLocations.size(); i++) {
          if (!blockLocations.get(i).getLocation().equals(Location.LOCAL)) {
            continue;
          }

          long block = startBlock + i;
          long startPosition = toBlockStartPosition(block, conf);
          long endPosition = Math.min(toBlockStartPosition(block + 1, conf), context.getFileSize());
          ReadRequest readRequest = new ReadRequest(startPosition, endPosition, startPosition, endPosition, null, 0, context.getFileSize());
          requestChain.addReadRequest(readRequest);
        }
      }

      if (requestChain != null) {
        log.debug("Request added for file: " + requestChain.getRemotePath() + " Number of Requests : " +
                requestChain.getReadRequests().size());

        readRequestChainList.add(requestChain);
      }
    }

    return readRequestChainList;
  }

  protected long processDownloadRequests(List<FileDownloadRequestChain> readRequestChainList)
  {
    if (readRequestChainList.size() == 0) {
      return 0;
    }

    long sizeRead = 0;
    List<Future<Long>> futures = new ArrayList<>();

    for (FileDownloadRequestChain requestChain : readRequestChainList) {
      requestChain.lock();
      Future<Long> result = processService.submit(requestChain);
      futures.add(result);
    }

    for (Future<Long> future : futures) {
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
        if (ex.getCause() instanceof FileNotFoundException) {
          log.debug("Could not process download request: " + ex.getCause().getMessage());
        } else {
          log.error("Could not process download request", ex);
        }
        requestChain.cancel();
      }
    }
    long dataDownloadedInMB = BYTES.toMB(sizeRead);
    this.totalMBDownloaded.inc(dataDownloadedInMB);
    return sizeRead;
  }
}
