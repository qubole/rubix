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

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Ticker;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.qubole.rubix.bookkeeper.exception.BookKeeperInitializationException;
import com.qubole.rubix.bookkeeper.exception.WorkerInitializationException;
import com.qubole.rubix.common.metrics.BookKeeperMetrics;
import com.qubole.rubix.common.utils.ClusterUtil;
import com.qubole.rubix.spi.BookKeeperFactory;
import com.qubole.rubix.spi.CacheConfig;
import com.qubole.rubix.spi.RetryingBookkeeperClient;
import com.qubole.rubix.spi.thrift.ClusterNode;
import com.qubole.rubix.spi.thrift.HeartbeatRequest;
import com.qubole.rubix.spi.thrift.NodeState;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class WorkerBookKeeper extends BookKeeper
{
  private static Log log = LogFactory.getLog(WorkerBookKeeper.class);
  private LoadingCache<String, List<ClusterNode>> nodeStateMap;
  private HeartbeatService heartbeatService;
  private BookKeeperFactory bookKeeperFactory;
  // The hostname of the master node.
  private String masterHostname;
  private RetryingBookkeeperClient client;

  public WorkerBookKeeper(Configuration conf, BookKeeperMetrics bookKeeperMetrics) throws BookKeeperInitializationException
  {
    this(conf, bookKeeperMetrics, Ticker.systemTicker(), new BookKeeperFactory());
  }

  public WorkerBookKeeper(Configuration conf, BookKeeperMetrics bookKeeperMetrics, BookKeeperFactory factory)
      throws BookKeeperInitializationException
  {
    this(conf, bookKeeperMetrics, Ticker.systemTicker(), factory);
  }

  public WorkerBookKeeper(Configuration conf, BookKeeperMetrics bookKeeperMetrics, Ticker ticker, BookKeeperFactory factory)
      throws BookKeeperInitializationException
  {
    super(conf, bookKeeperMetrics, ticker);
    if (factory == null) {
      this.bookKeeperFactory = new BookKeeperFactory(this);
    }
    else {
      this.bookKeeperFactory = factory;
    }

    this.masterHostname = ClusterUtil.getMasterHostname(conf);

    initializeWorker(conf, metrics, ticker, factory);
  }

  private void initializeWorker(Configuration conf, MetricRegistry metrics, Ticker ticker, BookKeeperFactory factory)
      throws WorkerInitializationException
  {
    startHeartbeatService(conf, metrics, factory);
    initializeNodeStateCache(conf, ticker);
    int errorCount = 0;
    int waitInterval = CacheConfig.getClusterNodesFetchWaitInterval(conf);
    int maxRetries = CacheConfig.getClusterNodesFetchRetryCount(conf);

    // Retry logic to fetch the list of nodes. This is needed as when the worker bookkeeper started, the nodes
    // in the cluster might not have registered with the master node. We will wait for some time to get that started.
    while (nodeName == null || nodeName.isEmpty()) {
      try {
        setCurrentNodeName(conf);
      }
      catch (WorkerInitializationException ex) {
        errorCount++;
        if (errorCount < maxRetries) {
          try {
            Thread.sleep(waitInterval);
          }
          catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        }
        else {
          throw ex;
        }
      }
    }
  }

  void setCurrentNodeName(Configuration conf) throws WorkerInitializationException
  {
    String nodeHostName;
    String nodeHostAddress;
    // If the node hostname is already set, honor that value.
    // In case of embedded mode, the engine might have set this value.
    if (CacheConfig.getCurrentNodeHostName(conf) != null) {
      nodeName = CacheConfig.getCurrentNodeHostName(conf);
      return;
    }
    try {
      nodeHostName = InetAddress.getLocalHost().getCanonicalHostName();
      nodeHostAddress = InetAddress.getLocalHost().getHostAddress();
      log.debug(" HostName : " + nodeHostName + " HostAddress : " + nodeHostAddress);
    }
    catch (UnknownHostException e) {
      log.warn("Could not get nodeName", e);
      throw new WorkerInitializationException("Could not get NodeName ", e);
    }

    List<ClusterNode> nodeList = getClusterNodes();

    if (nodeList != null && nodeList.size() > 0) {
      Set<String> nodeSet = new HashSet<>();
      for (ClusterNode node : nodeList) {
        if (node.nodeState == NodeState.ACTIVE) {
          nodeSet.add(node.nodeUrl);
        }
      }

      if (nodeSet.contains(nodeHostName)) {
        nodeName = nodeHostName;
      }
      else if (nodeSet.contains(nodeHostAddress)) {
        nodeName = nodeHostAddress;
      }
      else {
        String errorMessage = String.format("Could not find nodeHostName=%s nodeHostAddress=%s among cluster nodes=%s  " +
            "provided by master bookkeeper", nodeHostName, nodeHostAddress, nodeList);
        log.error(errorMessage);
        throw new WorkerInitializationException(errorMessage);
      }
    }
    else {
      String errorMessage = String.format("Could not find nodeHostName=%s nodeHostAddress=%s among cluster nodes=%s  " +
              "provided by master bookkeeper", nodeHostName, nodeHostAddress, nodeList);
      log.error(errorMessage);
      throw new WorkerInitializationException(errorMessage);
    }
  }

  private void initializeNodeStateCache(final Configuration conf, final Ticker ticker)
  {
    int expiryPeriod = CacheConfig.getWorkerNodeInfoExpiryPeriod(conf);
    nodeStateMap = CacheBuilder.newBuilder()
        .ticker(ticker)
        .refreshAfterWrite(expiryPeriod, TimeUnit.SECONDS)
        .build(new CacheLoader<String, List<ClusterNode>>() {
          @Override
          public List<ClusterNode> load(String s) throws Exception
          {
            if (client == null) {
              client = createBookKeeperClientWithRetry(bookKeeperFactory, masterHostname, conf);
            }
            try {
              return client.getClusterNodes();
            }
            catch (Exception e) {
              throw e;
            }
            finally {
              client.close();
              client = null;
            }
          }
        });
  }

  @Override
  public void handleHeartbeat(HeartbeatRequest request)
  {
    throw new UnsupportedOperationException("Worker node should not handle heartbeat");
  }

  @Override
  public List<ClusterNode> getClusterNodes()
  {
    try {
      return nodeStateMap.get("nodes");
    }
    catch (Exception e) {
      log.error("Could not get node host name from cache with Exception : " + e);
    }

    return null;
  }

  /**
   * Start the {@link HeartbeatService} for this worker node.
   */
  void startHeartbeatService(Configuration conf, MetricRegistry metrics, BookKeeperFactory factory)
  {
    if (CacheConfig.isHeartbeatEnabled(conf) || !CacheConfig.isEmbeddedModeEnabled(conf)) {
      this.heartbeatService = new HeartbeatService(conf, metrics, factory, this);
      heartbeatService.startAsync();
    }
  }

  /**
   * Attempt to initialize the client for communicating with the master BookKeeper.
   *
   * @param bookKeeperFactory   The factory to use for creating a BookKeeper client.
   * @return The client used for communication with the master node.
   */
  private static RetryingBookkeeperClient createBookKeeperClientWithRetry(BookKeeperFactory bookKeeperFactory,
                                                                          String hostName, Configuration conf)
  {
    final int retryInterval = CacheConfig.getServiceRetryInterval(conf);
    final int maxRetries = CacheConfig.getServiceMaxRetries(conf);

    return bookKeeperFactory.createBookKeeperClient(hostName, conf, maxRetries, retryInterval, false);
  }
}
