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

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Ticker;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.qubole.rubix.core.ClusterManagerInitilizationException;
import com.qubole.rubix.common.metrics.BookKeeperMetrics;
import com.qubole.rubix.spi.CacheConfig;
import com.qubole.rubix.spi.ClusterType;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import java.io.FileNotFoundException;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class CoordinatorBookKeeper extends BookKeeper
{
  private static Log log = LogFactory.getLog(CoordinatorBookKeeper.class.getName());

  // Cache to store hostnames of live workers in the cluster.
  protected Cache<String, Boolean> liveWorkerCache;

  public CoordinatorBookKeeper(Configuration conf, MetricRegistry metrics) throws FileNotFoundException
  {
    this(conf, metrics, Ticker.systemTicker());
  }

  @VisibleForTesting
  CoordinatorBookKeeper(Configuration conf, MetricRegistry metrics, Ticker ticker) throws FileNotFoundException
  {
    super(conf, metrics, ticker);
    this.liveWorkerCache = CacheBuilder.newBuilder()
        .ticker(ticker)
        .expireAfterWrite(CacheConfig.getWorkerLivenessExpiry(conf), TimeUnit.MILLISECONDS)
        .build();

    registerMetrics();
  }

  @Override
  public void handleHeartbeat(String workerHostname)
  {
    liveWorkerCache.put(workerHostname, true);
    log.debug("Received heartbeat from " + workerHostname);
  }

  @Override
  public List<String> getNodeHostNames(int clusterType)
  {
    try {
      initializeClusterManager(clusterType);
    }
    catch (ClusterManagerInitilizationException ex) {
      log.error("Not able to initialize ClusterManager for cluster type : " + ClusterType.findByValue(clusterType) +
          " with Exception : " + ex);
      return null;
    }

    log.debug("Fetching nodes from the cluster manager");
    List<String> nodes = clusterManager.getNodes();

    return nodes;
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

    List<String> nodes = getNodeHostNames(clusterType);
    int nodeIndex = clusterManager.getNodeIndex(nodes.size(), remotePath);
    String hostName = nodes.get(nodeIndex);

    return hostName;
  }

  /**
   * Register desired metrics.
   */
  private void registerMetrics()
  {
    metrics.register(BookKeeperMetrics.LivenessMetric.METRIC_BOOKKEEPER_LIVE_WORKER_GAUGE.getMetricName(), new Gauge<Integer>()
    {
      @Override
      public Integer getValue()
      {
        log.debug(String.format("Reporting %s workers", liveWorkerCache.asMap().size()));
        return liveWorkerCache.asMap().size();
      }
    });
  }
}
