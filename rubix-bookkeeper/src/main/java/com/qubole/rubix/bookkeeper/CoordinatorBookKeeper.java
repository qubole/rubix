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
import com.qubole.rubix.common.metrics.BookKeeperMetrics;
import com.qubole.rubix.spi.CacheConfig;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import java.io.FileNotFoundException;
import java.util.concurrent.TimeUnit;

public class CoordinatorBookKeeper extends BookKeeper
{
  private static Log log = LogFactory.getLog(CoordinatorBookKeeper.class.getName());

  // Cache to store hostnames of live workers in the cluster.
  protected Cache<String, Boolean> liveWorkerCache;

  // Cache to store hostnames of validated worker nodes.
  protected Cache<String, Boolean> validatedWorkerCache;

  public CoordinatorBookKeeper(Configuration conf, MetricRegistry metrics) throws FileNotFoundException
  {
    this(conf, metrics, Ticker.systemTicker());
  }

  @VisibleForTesting
  public CoordinatorBookKeeper(Configuration conf, MetricRegistry metrics, Ticker ticker) throws FileNotFoundException
  {
    super(conf, metrics, ticker);
    this.liveWorkerCache = CacheBuilder.newBuilder()
        .ticker(ticker)
        .expireAfterWrite(CacheConfig.getHealthStatusExpiry(conf), TimeUnit.MILLISECONDS)
        .build();
    this.validatedWorkerCache = CacheBuilder.newBuilder()
        .ticker(ticker)
        .expireAfterWrite(CacheConfig.getHealthStatusExpiry(conf), TimeUnit.MILLISECONDS)
        .build();

    registerMetrics();
  }

  @Override
  public void handleHeartbeat(String workerHostname, boolean didValidationSucceed)
  {
    liveWorkerCache.put(workerHostname, true);
    log.debug("Received heartbeat from " + workerHostname);

    if (didValidationSucceed) {
      validatedWorkerCache.put(workerHostname, true);
    }
    else {
      log.error(String.format("Caching behavior validation failed for worker node (hostname: %s)", workerHostname));
    }
  }

  /**
   * Register desired metrics.
   */
  private void registerMetrics()
  {
    metrics.register(BookKeeperMetrics.HealthMetric.LIVE_WORKER_GAUGE.getMetricName(), new Gauge<Long>()
    {
      @Override
      public Long getValue()
      {
        // Clean up cache to ensure accurate size is reported.
        liveWorkerCache.cleanUp();
        log.debug(String.format("Reporting %d workers", liveWorkerCache.size()));
        return liveWorkerCache.size();
      }
    });
    metrics.register(BookKeeperMetrics.HealthMetric.VALIDATED_WORKER_GAUGE.getMetricName(), new Gauge<Long>()
    {
      @Override
      public Long getValue()
      {
        // Clean up cache to ensure accurate size is reported.
        validatedWorkerCache.cleanUp();
        log.debug(String.format("Validated caching behavior for %d workers", validatedWorkerCache.size()));
        return validatedWorkerCache.size();
      }
    });
  }
}
