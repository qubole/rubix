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

package com.qubole.rubix.common.metrics;

import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ganglia.GangliaReporter;
import com.google.common.base.Splitter;
import com.qubole.rubix.common.utils.ClusterUtil;
import com.qubole.rubix.spi.CacheConfig;
import com.readytalk.metrics.StatsDReporter;
import info.ganglia.gmetric4j.gmetric.GMetric;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class BookKeeperMetrics implements AutoCloseable
{
  private static Log log = LogFactory.getLog(BookKeeperMetrics.class);

  private final Configuration conf;
  private final MetricRegistry metrics;
  private final BookKeeperMetricsFilter metricsFilter;
  protected final Set<Closeable> reporters = new HashSet<>();

  public BookKeeperMetrics(Configuration conf, MetricRegistry metrics)
  {
    this.conf = conf;
    this.metrics = metrics;
    this.metricsFilter = new BookKeeperMetricsFilter(conf);
    initializeReporters();
  }

  public BookKeeperMetricsFilter getMetricsFilter()
  {
    return metricsFilter;
  }

  public MetricRegistry getMetricsRegistry()
  {
    return metrics;
  }

  /**
   * Initialize reporters for reporting metrics to desired services.
   */
  protected void initializeReporters()
  {
    final Iterable<String> metricsReporterNames = Splitter.on(",").trimResults().omitEmptyStrings().split(CacheConfig.getMetricsReporters(conf));

    final Set<MetricsReporter> metricsReporters = new HashSet<>();
    for (String reporterName : metricsReporterNames) {
      metricsReporters.add(MetricsReporter.valueOf(reporterName.toUpperCase()));
    }

    for (MetricsReporter reporter : metricsReporters) {
      switch (reporter) {
        case JMX:
          final JmxReporter jmxReporter = JmxReporter.forRegistry(metrics)
              .convertRatesTo(TimeUnit.SECONDS)
              .convertDurationsTo(TimeUnit.MILLISECONDS)
              .filter(metricsFilter)
              .build();

          log.debug("Reporting metrics to JMX");
          jmxReporter.start();
          reporters.add(jmxReporter);
          break;
        case STATSD:
          if (!CacheConfig.isOnMaster(conf)) {
            CacheConfig.setStatsDMetricsHost(conf, ClusterUtil.getMasterHostname(conf));
          }
          final StatsDReporter statsDReporter = StatsDReporter.forRegistry(metrics)
              .convertRatesTo(TimeUnit.SECONDS)
              .convertDurationsTo(TimeUnit.MILLISECONDS)
              .filter(metricsFilter)
              .build(CacheConfig.getStatsDMetricsHost(conf), CacheConfig.getStatsDMetricsPort(conf));

          log.debug(String.format("Reporting metrics to StatsD [%s:%d]", CacheConfig.getStatsDMetricsHost(conf), CacheConfig.getStatsDMetricsPort(conf)));
          statsDReporter.start(CacheConfig.getMetricsReportingInterval(conf), TimeUnit.MILLISECONDS);
          reporters.add(statsDReporter);
          break;
        case GANGLIA:
          if (!CacheConfig.isOnMaster(conf)) {
            CacheConfig.setGangliaMetricsHost(conf, ClusterUtil.getMasterHostname(conf));
          }
          log.debug(String.format("Reporting metrics to Ganglia [%s:%s]", CacheConfig.getGangliaMetricsHost(conf), CacheConfig.getGangliaMetricsPort(conf)));
          final GMetric ganglia = new GMetric(CacheConfig.getGangliaMetricsHost(conf), CacheConfig.getGangliaMetricsPort(conf), GMetric.UDPAddressingMode.MULTICAST, 1);
          final GangliaReporter gangliaReporter = GangliaReporter.forRegistry(metrics)
                  .convertRatesTo(TimeUnit.SECONDS)
                  .convertDurationsTo(TimeUnit.MILLISECONDS)
                  .filter(metricsFilter)
                  .build(ganglia);
          gangliaReporter.start(CacheConfig.getMetricsReportingInterval(conf), TimeUnit.MILLISECONDS);
          reporters.add(gangliaReporter);
          break;
      }
    }
  }

  @Override
  public void close() throws IOException
  {
    for (Closeable reporter : reporters) {
      reporter.close();
    }
  }

  /**
   * Enum for metrics relating to JVM statistics on the BookKeeper daemon.
   */
  public enum BookKeeperJvmMetric
  {
    BOOKKEEPER_JVM_GC_PREFIX("rubix.bookkeeper.jvm.gc"),
    BOOKKEEPER_JVM_MEMORY_PREFIX("rubix.bookkeeper.jvm.memory"),
    BOOKKEEPER_JVM_THREADS_PREFIX("rubix.bookkeeper.jvm.threads");

    private final String metricName;

    BookKeeperJvmMetric(String metricName)
    {
      this.metricName = metricName;
    }

    public String getMetricName()
    {
      return metricName;
    }

    /**
     * Get the names for each BookKeeper JVM metric.
     *
     * @return a set of metrics names.
     */
    public static Set<String> getAllNames()
    {
      Set<String> names = new HashSet<>();
      for (BookKeeperJvmMetric metric : values()) {
        names.add(metric.getMetricName());
      }
      return names;
    }
  }

  /**
   * Enum for metrics relating to JVM statistics on the LocalDataTransferServer daemon.
   */
  public enum LDTSJvmMetric
  {
    LDTS_JVM_GC_PREFIX("rubix.ldts.jvm.gc"),
    LDTS_JVM_MEMORY_PREFIX("rubix.ldts.jvm.memory"),
    LDTS_JVM_THREADS_PREFIX("rubix.ldts.jvm.threads");

    private final String metricName;

    LDTSJvmMetric(String metricName)
    {
      this.metricName = metricName;
    }

    public String getMetricName()
    {
      return metricName;
    }

    /**
     * Get the names for each LDTS JVM metric.
     *
     * @return a set of metrics names.
     */
    public static Set<String> getAllNames()
    {
      Set<String> names = new HashSet<>();
      for (LDTSJvmMetric metric : values()) {
        names.add(metric.getMetricName());
      }
      return names;
    }
  }

  /**
   * Enum for metrics relating to cache interactions.
   */
  public enum CacheMetric
  {
    CACHE_EVICTION_COUNT("rubix.bookkeeper.count.cache_eviction"),
    CACHE_INVALIDATION_COUNT("rubix.bookkeeper.count.cache_invalidation"),
    CACHE_EXPIRY_COUNT("rubix.bookkeeper.count.cache_expiry"),
    CACHE_HIT_RATE_GAUGE("rubix.bookkeeper.gauge.cache_hit_rate"),
    CACHE_MISS_RATE_GAUGE("rubix.bookkeeper.gauge.cache_miss_rate"),
    CACHE_SIZE_GAUGE("rubix.bookkeeper.gauge.cache_size_mb"),
    CACHE_AVAILABLE_SIZE_GAUGE("rubix.bookkeeper.gauge.available_cache_size_mb"),
    TOTAL_REQUEST_COUNT("rubix.bookkeeper.count.total_request"),
    CACHE_REQUEST_COUNT("rubix.bookkeeper.count.cache_request"),
    NONLOCAL_REQUEST_COUNT("rubix.bookkeeper.count.nonlocal_request"),
    REMOTE_REQUEST_COUNT("rubix.bookkeeper.count.remote_request"),
    TOTAL_ASYNC_REQUEST_COUNT("rubix.bookkeeper.count.total_async_request"),
    PROCESSED_ASYNC_REQUEST_COUNT("rubix.bookkeeper.count.processed_async_request"),
    ASYNC_QUEUE_SIZE_GAUGE("rubix.bookkeeper.gauge.async_queue_size"),
    ASYNC_DOWNLOADED_MB_COUNT("rubix.bookkeeper.count.async_downloaded_mb"),
    ASYNC_DOWNLOAD_TIME_COUNT("rubix.bookkeeper.count.async_download_time");

    private final String metricName;

    CacheMetric(String metricName)
    {
      this.metricName = metricName;
    }

    public String getMetricName()
    {
      return metricName;
    }

    /**
     * Get the names for each cache metric.
     *
     * @return a set of metrics names.
     */
    public static Set<String> getAllNames()
    {
      Set<String> names = new HashSet<>();
      for (CacheMetric metric : values()) {
        names.add(metric.getMetricName());
      }
      return names;
    }
  }

  /**
   * Enum for metrics relating to daemon & service health.
   */
  public enum HealthMetric
  {
    LIVE_WORKER_GAUGE("rubix.bookkeeper.gauge.live_workers"),
    CACHING_VALIDATED_WORKER_GAUGE("rubix.bookkeeper.gauge.caching_validated_workers"),
    FILE_VALIDATED_WORKER_GAUGE("rubix.bookkeeper.gauge.file_validated_workers");

    private final String metricName;

    HealthMetric(String metricName)
    {
      this.metricName = metricName;
    }

    public String getMetricName()
    {
      return metricName;
    }

    /**
     * Get the names for each health metric.
     *
     * @return a set of metrics names.
     */
    public static Set<String> getAllNames()
    {
      Set<String> names = new HashSet<>();
      for (HealthMetric metric : values()) {
        names.add(metric.getMetricName());
      }
      return names;
    }
  }

  /**
   * Enum for metrics relating to validation.
   */
  public enum ValidationMetric
  {
    CACHING_VALIDATION_SUCCESS_GAUGE("rubix.bookkeeper.gauge.caching_validation_success"),
    FILE_VALIDATION_SUCCESS_GAUGE("rubix.bookkeeper.gauge.file_validation_success");

    private final String metricName;

    ValidationMetric(String metricName)
    {
      this.metricName = metricName;
    }

    public String getMetricName()
    {
      return metricName;
    }

    /**
     * Get the names for each health metric.
     *
     * @return a set of metrics names.
     */
    public static Set<String> getAllNames()
    {
      Set<String> names = new HashSet<>();
      for (ValidationMetric metric : values()) {
        names.add(metric.getMetricName());
      }
      return names;
    }
  }
}
