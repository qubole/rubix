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
import com.qubole.rubix.bookkeeper.exception.BookKeeperInitializationException;
import com.qubole.rubix.bookkeeper.exception.WorkerInitializationException;
import com.qubole.rubix.common.utils.ClusterUtil;
import com.qubole.rubix.spi.BookKeeperFactory;
import com.qubole.rubix.spi.CacheConfig;
import com.qubole.rubix.spi.RetryingBookkeeperClient;
import com.qubole.rubix.spi.thrift.HeartbeatStatus;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.shaded.TException;
import org.apache.thrift.shaded.transport.TTransportException;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.qubole.rubix.spi.ClusterType.TEST_CLUSTER_MANAGER;
import static com.qubole.rubix.spi.ClusterType.TEST_CLUSTER_MANAGER_MULTINODE;

public class WorkerBookKeeper extends BookKeeper
{
  private static Log log = LogFactory.getLog(WorkerBookKeeper.class);
  private LoadingCache<String, String> fileNodeMapCache;
  private HeartbeatService heartbeatService;
  private BookKeeperFactory bookKeeperFactory;
  private RetryingBookkeeperClient coordinatorClient;
  private ThreadLocal<RetryingBookkeeperClient> threadLocalConnections;
  // The hostname of the master node.
  private String masterHostname;
  private int clusterType;
  String nodeHostName;
  String nodeHostAddress;

  public WorkerBookKeeper(Configuration conf, MetricRegistry metrics) throws BookKeeperInitializationException
  {
    this(conf, metrics, Ticker.systemTicker(), new BookKeeperFactory());
  }

  public WorkerBookKeeper(Configuration conf, MetricRegistry metrics, Ticker ticker, BookKeeperFactory factory) throws BookKeeperInitializationException
  {
    super(conf, metrics, Ticker.systemTicker());
    this.bookKeeperFactory = factory;
    this.masterHostname = ClusterUtil.getMasterHostname(conf);
    this.clusterType = CacheConfig.getClusterType(conf);
    this.threadLocalConnections = new ThreadLocal<>();

    initializeWorker(conf, metrics, ticker, factory);
  }

  private void initializeWorker(Configuration conf, MetricRegistry metrics, Ticker ticker, BookKeeperFactory factory) throws WorkerInitializationException
  {
    setCurrentNodeName();
    startHeartbeatService(conf, metrics, factory);
    initializeFileNodeCache(conf, ticker);
  }

  void setCurrentNodeName() throws WorkerInitializationException
  {
    try {
      nodeHostName = InetAddress.getLocalHost().getCanonicalHostName();
      nodeHostAddress = InetAddress.getLocalHost().getHostAddress();
      log.info(" HostName : " + nodeHostName + " HostAddress : " + nodeHostAddress);
    }
    catch (UnknownHostException e) {
      log.warn("Could not get nodeName", e);
      throw new WorkerInitializationException("Could not get NodeName ", e);
    }

    try {
      List<String> nodes = getNodeHostNames();
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
    //ExecutorService executor = Executors.newSingleThreadExecutor();
    fileNodeMapCache = CacheBuilder.newBuilder()
        .ticker(ticker)
        .refreshAfterWrite(expiryPeriod, TimeUnit.SECONDS)
        .build(new CacheLoader<String, String>() {
          @Override
          public String load(String s) throws Exception
          {
            log.info("Fetching host name for " + s);
            RetryingBookkeeperClient client = threadLocalConnections.get();
            if (client == null) {
              client = bookKeeperFactory.createBookKeeperClient(masterHostname, conf);
              threadLocalConnections.set(client);
            }
            return client.getClusterNodeHostName(s);
          }
        });
  }

  public WorkerBookKeeper(Configuration conf, MetricRegistry metrics, BookKeeperFactory factory) throws BookKeeperInitializationException
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
    RetryingBookkeeperClient client = threadLocalConnections.get();
    try {
      if (client == null) {
        client = bookKeeperFactory.createBookKeeperClient(masterHostname, conf);
        threadLocalConnections.set(client);
      }

      return client.getNodeHostNames();
    }
    catch (TTransportException ex) {
      log.error("Not able to create client to fetch Node Names. Exception: " + ex);
    }

    return null;
  }

  @Override
  public String getClusterNodeHostName(String remotePath)
  {
    try {
      log.info("Size of cache " + fileNodeMapCache.size());
      log.info("Getting host name for path: " + remotePath);
      String hostName = fileNodeMapCache.get(remotePath);
      log.info("Got Host name for " + remotePath + " : " + hostName);
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
