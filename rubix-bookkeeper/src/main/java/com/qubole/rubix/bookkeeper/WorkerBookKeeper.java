/**
 * Copyright (c) 2018. Qubole Inc
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

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Ticker;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.AbstractScheduledService;
import com.google.common.util.concurrent.Service;
import com.qubole.rubix.core.ClusterManagerInitilizationException;
import com.qubole.rubix.core.utils.ClusterUtil;
import com.qubole.rubix.spi.BookKeeperFactory;
import com.qubole.rubix.spi.CacheConfig;
import com.qubole.rubix.spi.ClusterType;
import com.qubole.rubix.spi.RetryingBookkeeperClient;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.shaded.TException;
import org.apache.thrift.shaded.transport.TTransportException;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetAddress;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class WorkerBookKeeper extends BookKeeper
{
  private static Log log = LogFactory.getLog(WorkerBookKeeper.class.getName());
  private LoadingCache<Integer, List<String>> nodeListCache;
  private HeartbeatService heartbeatService;
  private BookKeeperFactory bookKeeperFactory;
  private RetryingBookkeeperClient client;
  // The hostname of the master node.
  private String masterHostname;

  public WorkerBookKeeper(Configuration conf, MetricRegistry metrics) throws FileNotFoundException
  {
    this(conf, metrics, Ticker.systemTicker());
  }

  public WorkerBookKeeper(Configuration conf, MetricRegistry metrics, Ticker ticker) throws FileNotFoundException
  {
    super(conf, metrics, Ticker.systemTicker());
    this.bookKeeperFactory = new BookKeeperFactory();
    this.masterHostname = ClusterUtil.getMasterHostname(conf);
    startHeartbeatService(conf);
    initializeNodesCache(conf, ticker);
  }

  private void initializeNodesCache(final Configuration conf, final Ticker ticker)
  {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    nodeListCache = CacheBuilder.newBuilder()
        .ticker(ticker)
        .expireAfterWrite(5, TimeUnit.MINUTES)
        .build(CacheLoader.asyncReloading(new CacheLoader<Integer, List<String>>() {
          @Override
          public List<String> load(Integer s) throws Exception
          {
            if (client == null) {
              client = initializeClientWithRetry(bookKeeperFactory, conf, masterHostname);
            }
            log.info("Fetching list of nodes for cluster type " + s.intValue() + " from master : " + masterHostname);
            return client.getNodeHostNames(s.intValue());
          }
        }, executor));
  }

  @Override
  public void handleHeartbeat(String workerHostname)
  {
    throw new UnsupportedOperationException("Worker node should not handle heartbeat");
  }

  @Override
  public List<String> getNodeHostNames(int clusterType)
  {
    throw new UnsupportedOperationException("Worker node should not return list of nodes");
  }

  @Override
  public String getClusterNodeHostName(String remotePath, int clusterType)
  {
    try {
      initializeClusterManager(clusterType);
    }
    catch (ClusterManagerInitilizationException ex) {
      log.error("Not able to initialize ClusterManager for cluster type : " + ClusterType.findByValue(clusterType) +
          " with Exception : " + ex);
      return null;
    }

    try {
      List<String> nodes = nodeListCache.get(clusterType);
      int nodeIndex = clusterManager.getNodeIndex(nodes.size(), remotePath);
      String hostName = nodes.get(nodeIndex);

      return hostName;
    }
    catch (ExecutionException e) {
      e.printStackTrace();
    }

    return null;
  }

  /**
   * Start the {@link HeartbeatService} for this worker node.
   */
  private void startHeartbeatService(Configuration conf)
  {
    this.heartbeatService = new HeartbeatService(conf, this.bookKeeperFactory);
    heartbeatService.startAsync();
  }

  /**
   * Attempt to initialize the client for communicating with the master BookKeeper.
   *
   * @param bookKeeperFactory   The factory to use for creating a BookKeeper client.
   * @return The client used for communication with the master node.
   */
  private static RetryingBookkeeperClient initializeClientWithRetry(BookKeeperFactory bookKeeperFactory, Configuration conf,
                                                             String hostName)
  {
    final int retryInterval = CacheConfig.getServiceRetryInterval(conf);
    final int maxRetries = CacheConfig.getServiceMaxRetries(conf);

    for (int failedStarts = 0; failedStarts < maxRetries; ) {
      try {
        return bookKeeperFactory.createBookKeeperClient(hostName, conf);
      }
      catch (TTransportException e) {
        log.warn("Could not start client for heartbeat service", e);
      }

      failedStarts++;
      if (failedStarts == maxRetries) {
        break;
      }

      try {
        Thread.sleep(retryInterval);
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    log.fatal("Heartbeat service ran out of retries to connect to the master BookKeeper");
    throw new RuntimeException("Could not start heartbeat service");
  }

  /**
   * Class to send a heartbeat to the master node in the cluster.
   */
  protected static class HeartbeatService extends AbstractScheduledService
  {
    private static final String KEY_MASTER_HOSTNAME = "master.hostname";
    private static final String KEY_YARN_RESOURCEMANAGER_ADDRESS = "yarn.resourcemanager.address";

    // The executor used for running listener callbacks.
    private final Executor executor = Executors.newSingleThreadExecutor();

    // The client for interacting with the master BookKeeper.
    private RetryingBookkeeperClient bookkeeperClient;

    // The initial delay for sending heartbeats.
    private final int heartbeatInitialDelay;

    // The interval at which to send a heartbeat.
    private final int heartbeatInterval;

    // The current Hadoop configuration.
    private final Configuration conf;

    private final String hostName;

    public HeartbeatService(Configuration conf, BookKeeperFactory bookKeeperFactory)
    {
      this.conf = conf;
      this.heartbeatInitialDelay = CacheConfig.getHeartbeatInitialDelay(conf);
      this.heartbeatInterval = CacheConfig.getHeartbeatInterval(conf);
      this.hostName = ClusterUtil.getMasterHostname(conf);
      this.bookkeeperClient = initializeClientWithRetry(bookKeeperFactory, conf, hostName);
    }

    @Override
    protected void startUp()
    {
      log.info(String.format("Starting service %s in thread %s", serviceName(), Thread.currentThread().getId()));
      addListener(new HeartbeatService.FailureListener(), executor);
    }

    @Override
    protected void runOneIteration()
    {
      try {
        log.debug(String.format("Sending heartbeat to %s", hostName));
        bookkeeperClient.handleHeartbeat(InetAddress.getLocalHost().getCanonicalHostName());
      }
      catch (IOException e) {
        log.error("Could not send heartbeat", e);
      }
      catch (TException te) {
        log.error(String.format("Could not connect to master node [%s]; will reattempt on next heartbeat", hostName));
      }
    }

    @Override
    protected AbstractScheduledService.Scheduler scheduler()
    {
      return AbstractScheduledService.Scheduler.newFixedDelaySchedule(heartbeatInitialDelay, heartbeatInterval, TimeUnit.MILLISECONDS);
    }

    /**
     * Listener to handle failures for this service.
     */
    private static class FailureListener extends com.google.common.util.concurrent.Service.Listener
    {
      @Override
      public void failed(Service.State from, Throwable failure)
      {
        super.failed(from, failure);
        log.error("Encountered a problem", failure);
      }
    }
  }
}
