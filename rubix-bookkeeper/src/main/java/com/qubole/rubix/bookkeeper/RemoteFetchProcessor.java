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
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.google.common.util.concurrent.AbstractScheduledService;
import com.qubole.rubix.common.metrics.BookKeeperMetrics;
import com.qubole.rubix.spi.CacheConfig;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class RemoteFetchProcessor extends AbstractScheduledService
{
  private Queue<FetchRequest> processQueue;
  private FileDownloader downloader;
  private MetricRegistry metrics;
  private Counter totalDownloadRequests;
  private Counter processedRequests;

  int processThreadInitalDelay;
  int processThreadInterval;
  long requestProcessDelay;

  private static final Log log = LogFactory.getLog(RemoteFetchProcessor.class);

  public RemoteFetchProcessor(BookKeeper bookKeeper, MetricRegistry metrics, Configuration conf)
  {
    // Initializing a new Config object so that it doesn't interfere with the existing one
    conf = new Configuration(conf);

    // Disabling FileSystem level cache for all the schemes.
    CacheConfig.disableFSCaches(conf);

    // Disable Rubix caching for cases we instantiate CachingFS objects to prevent loops
    CacheConfig.setCacheDataEnabled(conf, false);

    this.processQueue = new ConcurrentLinkedQueue<FetchRequest>();
    this.metrics = metrics;
    this.downloader = new FileDownloader(bookKeeper, metrics, conf);

    this.processThreadInitalDelay = CacheConfig.getProcessThreadInitialDelay(conf);
    this.processThreadInterval = CacheConfig.getProcessThreadInterval(conf);
    this.requestProcessDelay = CacheConfig.getRemoteFetchProcessInterval(conf);

    initializeMetrics();
  }

  FileDownloader getFileDownloaderInstance()
  {
    return downloader;
  }

  private void initializeMetrics()
  {
    totalDownloadRequests = metrics.counter(BookKeeperMetrics.CacheMetric.TOTAL_ASYNC_REQUEST_COUNT.getMetricName());
    processedRequests = metrics.counter(BookKeeperMetrics.CacheMetric.PROCESSED_ASYNC_REQUEST_COUNT.getMetricName());
    metrics.register(BookKeeperMetrics.CacheMetric.ASYNC_QUEUE_SIZE_GAUGE.getMetricName(), new Gauge<Integer>()
    {
      @Override
      public Integer getValue()
      {
        return processQueue.size();
      }
    });
  }

  public void addToProcessQueue(String remotePath, long offset, int length, long fileSize, long lastModified)
  {
    long requestedTime = System.currentTimeMillis();
    FetchRequest request = new FetchRequest(remotePath, offset, length, fileSize, lastModified, requestedTime);
    processQueue.add(request);
    totalDownloadRequests.inc();
  }

  @Override
  protected void runOneIteration() throws Exception
  {
    long currentTime = System.currentTimeMillis();

    if (!processQueue.isEmpty()) {
      processRequest(currentTime);
    }
  }

  protected void processRequest(long currentTime) throws IOException, InterruptedException, ExecutionException
  {
    ConcurrentMap<String, DownloadRequestContext> contextMap = mergeRequests(currentTime);
    processRemoteFetchRequest(contextMap);

    // After every iteration we are clearing the map
    contextMap.clear();
  }

  protected ConcurrentMap<String, DownloadRequestContext> mergeRequests(long currentTime)
  {
    // Till the queue is not empty or there are no more requests which came in before the configured delay time
    // we are going to collect the requests and process them
    ConcurrentMap<String, DownloadRequestContext> contextMap = new ConcurrentHashMap<String, DownloadRequestContext>();
    while (!processQueue.isEmpty()) {
      FetchRequest request = processQueue.peek();
      if (currentTime - request.getRequestedTime() < this.requestProcessDelay) {
        break;
      }

      DownloadRequestContext context = new DownloadRequestContext(request.getRemotePath(), request.getFileSize(),
          request.getLastModified());
      if (!contextMap.containsKey(request.getRemotePath())) {
        contextMap.putIfAbsent(request.getRemotePath(), context);
      }
      else {
        // This takes care of the case where the last modfied time of a file in the request is not matching
        // with the same of other requests. We will take the latest modified time as a source of truth.
        // If the last modfied time in context is less than that of current request, we will remove it from the map
        // Else we will ignore this request.
        if (contextMap.get(request.getRemotePath()).getLastModifiedTime() < request.getLastModified()) {
          contextMap.remove(request.getRemotePath());
          contextMap.putIfAbsent(request.getRemotePath(), context);
        }
        else if (contextMap.get(request.getRemotePath()).getLastModifiedTime() > request.getLastModified()) {
          // TODO add metric to track ignored requests
          processQueue.remove();
          continue;
        }
      }
      contextMap.get(request.getRemotePath()).addDownloadRange(request.getOffset(),
          request.getOffset() + request.getLength());
      processQueue.remove();
      processedRequests.inc();
    }

    return contextMap;
  }

  @Override
  protected Scheduler scheduler()
  {
    return Scheduler.newFixedDelaySchedule(processThreadInitalDelay, processThreadInterval, TimeUnit.MILLISECONDS);
  }

  private void processRemoteFetchRequest(ConcurrentMap<String, DownloadRequestContext> contextMap)
                                        throws IOException, InterruptedException, ExecutionException
  {
    List<FileDownloadRequestChain> readRequestChainList = downloader.getFileDownloadRequestChains(contextMap);
    downloader.processDownloadRequests(readRequestChainList);
  }
}
