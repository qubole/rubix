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

package com.qubole.rubix.bookkeeper.metrics;

import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Splitter;
import com.qubole.rubix.core.utils.ClusterUtil;
import com.qubole.rubix.spi.CacheConfig;
import com.readytalk.metrics.StatsDReporter;
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

  private final MetricRegistry metrics;
  private final Configuration conf;
  protected final Set<Closeable> reporters = new HashSet<>();

  public BookKeeperMetrics(Configuration conf, MetricRegistry metrics)
  {
    this.conf = conf;
    this.metrics = metrics;
    initializeReporters();
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
              .filter(new BookKeeperMetricsFilter(conf))
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
              .filter(new BookKeeperMetricsFilter(conf))
              .build(CacheConfig.getStatsDMetricsHost(conf), CacheConfig.getStatsDMetricsPort(conf));

          log.info(String.format("Reporting metrics to StatsD [%s:%s]", CacheConfig.getStatsDMetricsHost(conf), CacheConfig.getStatsDMetricsPort(conf)));
          statsDReporter.start(CacheConfig.getStatsDMetricsInterval(conf), TimeUnit.MILLISECONDS);
          reporters.add(statsDReporter);
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
    METRIC_BOOKKEEPER_JVM_GC_PREFIX("rubix.bookkeeper.gc"),
    METRIC_BOOKKEEPER_JVM_MEMORY_PREFIX("rubix.bookkeeper.memory"),
    METRIC_BOOKKEEPER_JVM_THREADS_PREFIX("rubix.bookkeeper.threads");

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
    METRIC_LDTS_JVM_GC_PREFIX("rubix.ldts.gc"),
    METRIC_LDTS_JVM_MEMORY_PREFIX("rubix.ldts.memory"),
    METRIC_LDTS_JVM_THREADS_PREFIX("rubix.ldts.threads");

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
    METRIC_BOOKKEEPER_LOCAL_CACHE_COUNT("rubix.bookkeeper.local_cache.count");

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
    METRIC_BOOKKEEPER_LIVENESS_CHECK("rubix.bookkeeper.liveness.gauge"),
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
