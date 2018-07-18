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

import com.codahale.metrics.Metric;
import com.google.common.collect.Sets;
import com.qubole.rubix.spi.CacheConfig;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Set;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestBookKeeperMetricsFilter
{
  private static final Log log = LogFactory.getLog(TestBookKeeperMetricsFilter.class);

  // Dummy metric required by MetricFilter interface, but not used in our MetricFilter implementation.
  private static final Metric TEST_METRIC = new Metric(){};

  private BookKeeperMetricsFilter metricsFilter;
  private final Configuration conf = new Configuration();

  @BeforeMethod
  public void setUp()
  {
    metricsFilter = new BookKeeperMetricsFilter(conf);
  }

  @AfterMethod
  public void tearDown()
  {
    conf.clear();
  }

  /**
   * Verify that the metrics filter matches against liveness metrics when they are enabled.
   */
  @Test
  public void testMatches_livenessMetricsEnabled()
  {
    CacheConfig.setLivenessMetricsEnabled(conf, true);
    Set<String> livenessMetricsNames = BookKeeperMetrics.LivenessMetric.getAllNames();

    for (String metricsName : livenessMetricsNames) {
      assertTrue(metricsFilter.matches(metricsName, TEST_METRIC));
    }
  }

  /**
   * Verify that the metrics filter does not match against liveness metrics when they are disabled.
   */
  @Test
  public void testMatches_livenessMetricsDisabled()
  {
    CacheConfig.setLivenessMetricsEnabled(conf, false);
    Set<String> livenessMetricsNames = BookKeeperMetrics.LivenessMetric.getAllNames();

    for (String metricsName : livenessMetricsNames) {
      assertFalse(metricsFilter.matches(metricsName, TEST_METRIC));
    }
  }

  /**
   * Verify that the metrics filter matches against cache metrics when they are enabled.
   */
  @Test
  public void testMatches_cacheMetricsEnabled()
  {
    CacheConfig.setCacheMetricsEnabled(conf, true);
    Set<String> cacheMetricsNames = BookKeeperMetrics.CacheMetric.getAllNames();

    for (String metricsName : cacheMetricsNames) {
      assertTrue(metricsFilter.matches(metricsName, TEST_METRIC));
    }
  }

  /**
   * Verify that the metrics filter does not match against cache metrics when they are disabled.
   */
  @Test
  public void testMatches_cacheMetricsDisabled()
  {
    CacheConfig.setCacheMetricsEnabled(conf, false);
    Set<String> cacheMetricsNames = BookKeeperMetrics.CacheMetric.getAllNames();

    for (String metricsName : cacheMetricsNames) {
      assertFalse(metricsFilter.matches(metricsName, TEST_METRIC));
    }
  }

  /**
   * Verify that the metrics filter matches against JVM metrics when they are enabled.
   */
  @Test
  public void testMatches_jvmMetricsEnabled()
  {
    CacheConfig.setJvmMetricsEnabled(conf, true);
    Set<String> cacheMetricsNames = Sets.union(
        BookKeeperMetrics.BookKeeperJvmMetric.getAllNames(),
        BookKeeperMetrics.LDTSJvmMetric.getAllNames());

    for (String metricsName : cacheMetricsNames) {
      assertTrue(metricsFilter.matches(metricsName, TEST_METRIC));
    }
  }

  /**
   * Verify that the metrics filter does not match against JVM metrics when they are disabled.
   */
  @Test
  public void testMatches_jvmMetricsDisabled()
  {
    CacheConfig.setJvmMetricsEnabled(conf, false);
    Set<String> cacheMetricsNames = Sets.union(
        BookKeeperMetrics.BookKeeperJvmMetric.getAllNames(),
        BookKeeperMetrics.LDTSJvmMetric.getAllNames());

    for (String metricsName : cacheMetricsNames) {
      assertFalse(metricsFilter.matches(metricsName, TEST_METRIC));
    }
  }
}
