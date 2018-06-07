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

package com.qubole.rubix.bookkeeper.manager;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.qubole.rubix.spi.CacheConfig;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Class to manage components on the coordinator node.
 */
public class CoordinatorManager implements NodeManager
{
  private static Log log = LogFactory.getLog(CoordinatorManager.class.getName());

  // Metric key for the number of live workers in the cluster.
  public static final String METRIC_BOOKKEEPER_LIVE_WORKER_GAUGE = "rubix.bookkeeper.live-workers.gauge";

  // Cache to store hostnames of live workers in the cluster.
  private final Cache<String, Boolean> liveWorkerCache;

  // Registry for gathering & storing necessary metrics.
  private final MetricRegistry metrics;

  // The initial delay for reporting the number of live workers in the cluster.
  private final int workerLivenessMetricInitialDelay;

  // The interval at which to report the number of live workers in the cluster.
  private final int workerLivenessMetricInterval;

  public CoordinatorManager(Configuration conf, MetricRegistry metrics)
  {
    this.metrics = metrics;
    this.liveWorkerCache = CacheBuilder.newBuilder()
        .expireAfterWrite(CacheConfig.getWorkerLivenessExpiry(conf), TimeUnit.MILLISECONDS)
        .build();
    this.workerLivenessMetricInitialDelay = CacheConfig.getWorkerLivenessMetricInitialDelay(conf);
    this.workerLivenessMetricInterval = CacheConfig.getWorkerLivenessMetricInterval(conf);

    scheduleWorkerCountMetric();
  }

  /**
   * Handle the heartbeat received from a worker node.
   *
   * @param workerHostname The hostname for the worker node.
   */
  public void handleHeartbeat(String workerHostname)
  {
    liveWorkerCache.put(workerHostname, true);
    log.info("Received heartbeat from " + workerHostname);
  }

  /**
   * Schedule a service to update the live worker count metric periodically.
   */
  private void scheduleWorkerCountMetric()
  {
    ScheduledExecutorService metricRegisterService = Executors.newSingleThreadScheduledExecutor();
    metricRegisterService.scheduleAtFixedRate(new Runnable()
    {
      @Override
      public void run()
      {
        updateWorkerCountMetric();
        log.info("Registered live worker count: " + liveWorkerCache.asMap().size() + " workers");
      }
    }, workerLivenessMetricInitialDelay, workerLivenessMetricInterval, TimeUnit.MILLISECONDS);
  }

  /**
   * Update the value of the live worker count metric.
   */
  private void updateWorkerCountMetric()
  {
    metrics.remove(METRIC_BOOKKEEPER_LIVE_WORKER_GAUGE);
    metrics.register(METRIC_BOOKKEEPER_LIVE_WORKER_GAUGE, new Gauge<Integer>()
    {
      @Override
      public Integer getValue()
      {
        return liveWorkerCache.asMap().size();
      }
    });
  }
}
