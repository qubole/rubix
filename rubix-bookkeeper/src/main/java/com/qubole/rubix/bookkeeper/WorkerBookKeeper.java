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
import com.qubole.rubix.bookkeeper.exception.WorkerInitializationException;
import com.qubole.rubix.common.utils.ClusterUtil;
import com.qubole.rubix.spi.BookKeeperFactory;
import com.qubole.rubix.spi.CacheConfig;
import com.qubole.rubix.spi.CacheUtil;
import com.qubole.rubix.spi.RetryingBookkeeperClient;
import com.qubole.rubix.spi.thrift.HeartbeatStatus;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.shaded.TException;

import java.io.FileNotFoundException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.qubole.rubix.spi.ClusterType.TEST_CLUSTER_MANAGER;
import static com.qubole.rubix.spi.ClusterType.TEST_CLUSTER_MANAGER_MULTINODE;

public class WorkerBookKeeper extends BookKeeper
{
  private static Log log = LogFactory.getLog(WorkerBookKeeper.class);
  private LoadingCache<String, String> fileNodeMapCache;
  private HeartbeatService heartbeatService;
  private BookKeeperFactory bookKeeperFactory;
  private RetryingBookkeeperClient client;
  // The hostname of the master node.
  private String masterHostname;
  private int clusterType;
  String nodeHostName;
  String nodeHostAddress;

  public WorkerBookKeeper(Configuration conf, MetricRegistry metrics) throws WorkerInitializationException
  {
    this(conf, metrics, Ticker.systemTicker(), new BookKeeperFactory());
  }

  public WorkerBookKeeper(Configuration conf, MetricRegistry metrics, Ticker ticker, BookKeeperFactory factory) throws WorkerInitializationException
  {
    super(conf, metrics, Ticker.systemTicker());

    try {
      CacheUtil.createCacheDirectories(conf);
      this.bookKeeperFactory = factory;
      this.masterHostname = ClusterUtil.getMasterHostname(conf);
      this.clusterType = CacheConfig.getClusterType(conf);

      initializeWorker();
      startHeartbeatService(conf, metrics, factory);
      initializeFileNodeCache(conf, ticker);
    }
    catch (FileNotFoundException ex) {
      throw new WorkerInitializationException(ex.toString(), ex);
    }
  }

  private void initializeWorker() throws WorkerInitializationException
  {
    setCurrentNodeNameAndIndex();
  }

  void setCurrentNodeNameAndIndex() throws WorkerInitializationException
  {
    if (client == null) {
      client = initializeClientWithRetry(bookKeeperFactory, conf, masterHostname);
    }

    if (client == null) {
      throw new WorkerInitializationException("Not able to open connection to master bookkeeper");
    }

    try {
      List<String> nodes = client.getNodeHostNames();
      if (clusterType == TEST_CLUSTER_MANAGER.ordinal() || clusterType == TEST_CLUSTER_MANAGER_MULTINODE.ordinal()) {
        nodeName = nodes.get(0);
        return;
      }

      if (nodes != null && nodes.size() > 0) {
        if (nodes.indexOf(nodeHostName) >= 0) {
          nodeName = nodeHostName;
        }
        else if (nodes.indexOf(nodeHostAddress) >= 0) {
          nodeName = nodeHostAddress;
        }
      }
      else {
        String errorMessage = String.format("Could not find nodeHostName=%s nodeHostAddress=%s among cluster nodes=%s  " +
                "provide by master bookkeeper", nodeHostName, nodeHostAddress, nodes);
        log.error(errorMessage);
        throw new WorkerInitializationException(errorMessage);
      }
    }
    catch (TException ex) {
      throw new WorkerInitializationException("Not able to get list of nodes from master");
    }
  }

  private void initializeFileNodeCache(final Configuration conf, final Ticker ticker)
  {
    int expiryPeriod = CacheConfig.getWorkerNodeInfoExpiryPeriod(conf);
    ExecutorService executor = Executors.newSingleThreadExecutor();
    fileNodeMapCache = CacheBuilder.newBuilder()
        .ticker(ticker)
        .refreshAfterWrite(expiryPeriod, TimeUnit.SECONDS)
        .build(CacheLoader.asyncReloading(new CacheLoader<String, String>() {
          @Override
          public String load(String s) throws Exception
          {
            if (client == null) {
              client = initializeClientWithRetry(bookKeeperFactory, conf, masterHostname);
            }
            if (client == null) {
              log.error("Not able to initialize bookkeeper client.");
              throw new Exception("Not able to initialize bookkeeper client");
            }

            return client.getClusterNodeHostName(s, 1);
          }
        }, executor));
  }

  public WorkerBookKeeper(Configuration conf, MetricRegistry metrics, BookKeeperFactory factory) throws WorkerInitializationException
  {
    this(conf, metrics, Ticker.systemTicker(), factory);
  }

  @Override
  public void handleHeartbeat(String workerHostname, HeartbeatStatus request)
  {
    throw new UnsupportedOperationException("Worker node should not handle heartbeat");
  }

  @Override
  public List<String> getNodeHostNames() throws TException
  {
    return client.getNodeHostNames();
  }

  @Override
  public String getClusterNodeHostName(String remotePath, int clusterType)
  {
    try {
      String hostName = fileNodeMapCache.get(remotePath);
      return hostName;
    }
    catch (ExecutionException e) {
      log.error("Could not get node host name from cache with Exception : " + e);
    }

    return null;
  }

  /**
   * Start the {@link HeartbeatService} for this worker node.
   */
  void startHeartbeatService(Configuration conf, MetricRegistry metrics, BookKeeperFactory factory)
  {
    this.heartbeatService = new HeartbeatService(conf, metrics, factory, this);
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

    return bookKeeperFactory.createBookKeeperClient(hostName, conf, maxRetries, retryInterval, false);
  }
}
