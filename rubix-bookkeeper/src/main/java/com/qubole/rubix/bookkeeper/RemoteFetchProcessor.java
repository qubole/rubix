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

import com.google.common.util.concurrent.AbstractScheduledService;
import com.qubole.rubix.core.FileDownloadRequestChain;
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
  private Configuration conf;
  private Queue<FetchRequest> processQueue;
  private FileDownloader downloader;

  int processThreadInitalDelay;
  int processThreadInterval;
  long requestProcessDelay;

  private static final Log log = LogFactory.getLog(RemoteFetchProcessor.class);

  public RemoteFetchProcessor(Configuration conf)
  {
    this.conf = conf;
    this.processQueue = new ConcurrentLinkedQueue<FetchRequest>();
    this.downloader = new FileDownloader(conf);

    this.processThreadInitalDelay = CacheConfig.getProcessThreadInitialDelay(conf);
    this.processThreadInterval = CacheConfig.getProcessThreadInterval(conf);
    this.requestProcessDelay = CacheConfig.getRemoteFetchProcessInterval(conf);
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
          continue;
        }
      }
      contextMap.get(request.getRemotePath()).addDownloadRange(request.getOffset(),
          request.getOffset() + request.getLength());
      processQueue.remove();
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
