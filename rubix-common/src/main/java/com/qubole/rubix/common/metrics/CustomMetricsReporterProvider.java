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

import com.codahale.metrics.MetricRegistry;
import com.qubole.rubix.spi.CacheConfig;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static com.qubole.rubix.common.utils.ClusterUtil.getMetricsReporters;

public class CustomMetricsReporterProvider {
  private static final Log log = LogFactory.getLog(CustomMetricsReporterProvider.class);

  private static volatile AtomicReference<Boolean> reporterRunning = new AtomicReference<>();

  private CustomMetricsReporter customMetricsReporter;
  private static volatile CustomMetricsReporterProvider customMetricsReporterProvider;

  private CustomMetricsReporterProvider(CustomMetricsReporter customMetricsReporter) {
      this.customMetricsReporter = customMetricsReporter;
  }

  public static void initialize(Configuration configuration)
  {
      initialize(configuration, Optional.empty());
  }

  public static void initialize(Configuration configuration, Optional<MetricRegistry> metricRegistry) {
    if (customMetricsReporterProvider == null) {
      synchronized (CustomMetricsReporterProvider.class) {
        if (customMetricsReporterProvider == null) {
          String className = CacheConfig.getRubixMetricCollectorImpl(configuration);
          // check if custom reporter is enabled: Check here for CFS metrics Reporter.
          boolean useCustomReporter = getMetricsReporters(configuration).contains(MetricsReporterType.CUSTOM);
          CustomMetricsReporter customMetricsReporter;
          if (useCustomReporter && !className.equals("com.qubole.rubix.common.metrics.NoOpReporter")) {
            try {
              Class collectorClass = Class.forName(className);
              log.info(String.format("Using class for metric reporting: %s", className));
              customMetricsReporter = (CustomMetricsReporter) collectorClass.getDeclaredConstructor(Configuration.class, Optional.class)
                      .newInstance(configuration, metricRegistry);
            } catch (Exception e) {
              log.warn("External Metric Reporter class: %s can not be initialized: ", e);
              customMetricsReporter = new NoOpReporter();
            }
          } else {
            customMetricsReporter = new NoOpReporter();
          }
          customMetricsReporterProvider = new CustomMetricsReporterProvider(customMetricsReporter);
        }
      }
    }
  }

  public static CustomMetricsReporter getCustomMetricsReporter() {
    if (reporterRunning.get() == null) {
      synchronized (reporterRunning) {
        if (reporterRunning.get() == null) {
          try {
            customMetricsReporterProvider.customMetricsReporter.start();
          } catch (Exception e) {
            log.warn("Exception in starting Custom reporter: ", e);
            customMetricsReporterProvider.customMetricsReporter = new NoOpReporter();
          }
          reporterRunning.set(true);
        }
      }
    }
    return customMetricsReporterProvider.customMetricsReporter;
  }
}
