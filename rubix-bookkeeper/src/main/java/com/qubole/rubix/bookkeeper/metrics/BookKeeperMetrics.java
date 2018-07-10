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
}
