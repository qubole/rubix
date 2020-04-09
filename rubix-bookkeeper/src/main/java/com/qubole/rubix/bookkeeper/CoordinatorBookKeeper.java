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

import com.codahale.metrics.Gauge;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Ticker;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.qubole.rubix.common.metrics.BookKeeperMetrics;
import com.qubole.rubix.spi.CacheConfig;
import com.qubole.rubix.spi.thrift.HeartbeatStatus;
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

  // Cache to store hostnames of caching-validated worker nodes.
  protected Cache<String, Boolean> cachingValidatedWorkerCache;

  // Cache to store hostnames of file-validated worker nodes.
  protected Cache<String, Boolean> fileValidatedWorkerCache;

  private final boolean isValidationEnabled;

  public CoordinatorBookKeeper(Configuration conf, BookKeeperMetrics bookKeeperMetrics) throws FileNotFoundException
  {
    this(conf, bookKeeperMetrics, Ticker.systemTicker());
  }

  @VisibleForTesting
  public CoordinatorBookKeeper(Configuration conf, BookKeeperMetrics bookKeeperMetrics, Ticker ticker) throws FileNotFoundException
  {
    super(conf, bookKeeperMetrics, ticker);
    this.isValidationEnabled = CacheConfig.isValidationEnabled(conf);
    this.liveWorkerCache = createHealthCache(conf, ticker);
    this.cachingValidatedWorkerCache = createHealthCache(conf, ticker);
    this.fileValidatedWorkerCache = createHealthCache(conf, ticker);

    registerMetrics();
  }

  @Override
  public void handleHeartbeat(String workerHostname, HeartbeatStatus heartbeatStatus)
  {
    if (CacheConfig.isHeartbeatEnabled(conf) || !CacheConfig.isEmbeddedModeEnabled(conf)) {
      liveWorkerCache.put(workerHostname, true);
      log.debug("Received heartbeat from " + workerHostname);

      if (isValidationEnabled) {
        if (heartbeatStatus.cachingValidationSucceeded) {
          cachingValidatedWorkerCache.put(workerHostname, true);
        }
        else {
          log.error(String.format("Caching validation failed for worker node (hostname: %s)", workerHostname));
        }

        if (heartbeatStatus.fileValidationSucceeded) {
          fileValidatedWorkerCache.put(workerHostname, true);
        }
        else {
          log.error(String.format("File validation failed for worker node (hostname: %s)", workerHostname));
        }
      }
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
        log.debug(String.format("Reporting %d live workers", liveWorkerCache.size()));
        return liveWorkerCache.size();
      }
    });

    if (isValidationEnabled) {
      metrics.register(BookKeeperMetrics.HealthMetric.CACHING_VALIDATED_WORKER_GAUGE.getMetricName(), new Gauge<Long>()
      {
        @Override
        public Long getValue()
        {
          // Clean up cache to ensure accurate size is reported.
          cachingValidatedWorkerCache.cleanUp();
          log.debug(String.format("Caching validation passed for %d workers", cachingValidatedWorkerCache.size()));
          return cachingValidatedWorkerCache.size();
        }
      });
      metrics.register(BookKeeperMetrics.HealthMetric.FILE_VALIDATED_WORKER_GAUGE.getMetricName(), new Gauge<Long>()
      {
        @Override
        public Long getValue()
        {
          // Clean up cache to ensure accurate size is reported.
          fileValidatedWorkerCache.cleanUp();
          log.debug(String.format("File validation passed for %d workers", fileValidatedWorkerCache.size()));
          return fileValidatedWorkerCache.size();
        }
      });
    }
  }

  /**
   * Create a cache for storing the status of worker node health checks.
   *
   * @param conf    The current Hadoop configuration.
   * @param ticker  The ticker used for determining expiry of cache entries.
   * @return a cache reflecting the health status of worker nodes.
   */
  private Cache<String, Boolean> createHealthCache(Configuration conf, Ticker ticker)
  {
    return CacheBuilder.newBuilder()
        .ticker(ticker)
        .expireAfterWrite(CacheConfig.getHealthStatusExpiry(conf), TimeUnit.MILLISECONDS)
        .build();
  }
}
