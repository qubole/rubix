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

package com.qubole.rubix.common.metrics;

import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ganglia.GangliaReporter;
import com.google.common.base.Splitter;
import com.qubole.rubix.core.utils.ClusterUtil;
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

          log.info("Reporting metrics to JMX");
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

          log.info(String.format("Reporting metrics to StatsD [%s:%s]", CacheConfig.getStatsDMetricsHost(conf), CacheConfig.getStatsDMetricsPort(conf)));
          statsDReporter.start(CacheConfig.getStatsDMetricsInterval(conf), TimeUnit.MILLISECONDS);
          reporters.add(statsDReporter);
          break;
        case GANGLIA:
          if (!CacheConfig.isOnMaster(conf)) {
            CacheConfig.setGangliaDMetricsHost(conf, ClusterUtil.getMasterHostname(conf));
          }
          log.info(String.format("Reporting metrics to Ganglia [%s:%s]", CacheConfig.getStatsDMetricsHost(conf), CacheConfig.getGangliaMetricsPort(conf)));
          final GMetric ganglia = new GMetric(CacheConfig.getStatsDMetricsHost(conf), CacheConfig.getGangliaMetricsPort(conf), GMetric.UDPAddressingMode.MULTICAST, 1);
          final GangliaReporter gangliaReporter = GangliaReporter.forRegistry(metrics)
                  .convertRatesTo(TimeUnit.SECONDS)
                  .convertDurationsTo(TimeUnit.MILLISECONDS)
                  .build(ganglia);
          gangliaReporter.start(CacheConfig.getStatsDMetricsInterval(conf), TimeUnit.MILLISECONDS);
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
    METRIC_BOOKKEEPER_JVM_GC_PREFIX("rubix.bookkeeper.jvm.gc"),
    METRIC_BOOKKEEPER_JVM_MEMORY_PREFIX("rubix.bookkeeper.jvm.memory"),
    METRIC_BOOKKEEPER_JVM_THREADS_PREFIX("rubix.bookkeeper.jvm.threads");

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
    METRIC_LDTS_JVM_GC_PREFIX("rubix.ldts.jvm.gc"),
    METRIC_LDTS_JVM_MEMORY_PREFIX("rubix.ldts.jvm.memory"),
    METRIC_LDTS_JVM_THREADS_PREFIX("rubix.ldts.jvm.threads");

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
    METRIC_BOOKKEEPER_CACHE_EVICTION_COUNT("rubix.bookkeeper.cache_eviction.count"),
    METRIC_BOOKKEEPER_CACHE_INVALIDATION_COUNT("rubix.bookkeeper.cache_invalidation.count"),
    METRIC_BOOKKEEPER_CACHE_EXPIRY_COUNT("rubix.bookkeeper.cache_expiry.count"),
    METRIC_BOOKKEEPER_CACHE_HIT_RATE_GAUGE("rubix.bookkeeper.cache_hit_rate.gauge"),
    METRIC_BOOKKEEPER_CACHE_MISS_RATE_GAUGE("rubix.bookkeeper.cache_miss_rate.gauge"),
    METRIC_BOOKKEEPER_CACHE_SIZE_GAUGE("rubix.bookkeeper.cache_size_mb.gauge"),
    METRIC_BOOKKEEPER_TOTAL_REQUEST_COUNT("rubix.bookkeeper.total_request.count"),
    METRIC_BOOKKEEPER_CACHE_REQUEST_COUNT("rubix.bookkeeper.cache_request.count"),
    METRIC_BOOKKEEPER_NONLOCAL_REQUEST_COUNT("rubix.bookkeeper.nonlocal_request.count"),
    METRIC_BOOKKEEPER_REMOTE_REQUEST_COUNT("rubix.bookkeeper.remote_request.count");

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
   * Enum for metrics relating to daemon & service liveness.
   */
  public enum LivenessMetric
  {
    METRIC_BOOKKEEPER_LIVE_WORKER_GAUGE("rubix.bookkeeper.live_workers.gauge");

    private final String metricName;

    LivenessMetric(String metricName)
    {
      this.metricName = metricName;
    }

    public String getMetricName()
    {
      return metricName;
    }

    /**
     * Get the names for each liveness metric.
     *
     * @return a set of metrics names.
     */
    public static Set<String> getAllNames()
    {
      Set<String> names = new HashSet<>();
      for (LivenessMetric metric : values()) {
        names.add(metric.getMetricName());
      }
      return names;
    }
  }
}
