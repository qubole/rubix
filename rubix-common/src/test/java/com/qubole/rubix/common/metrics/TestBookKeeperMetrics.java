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
import com.google.common.base.Joiner;
import com.google.common.collect.Sets;
import com.qubole.rubix.common.TestUtil;
import com.qubole.rubix.spi.CacheConfig;
import com.readytalk.metrics.StatsDReporter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Set;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestBookKeeperMetrics
{
  private static final Log log = LogFactory.getLog(TestBookKeeperMetrics.class);

  private static final String TEST_CACHE_DIR_PREFIX = TestUtil.getTestCacheDirPrefix("TestBookKeeperMetrics");
  private static final int TEST_BLOCK_SIZE = 100;
  private static final int TEST_MAX_DISKS = 1;

  private final Configuration conf = new Configuration();
  private MetricRegistry metrics;

  @BeforeClass
  public void setUpForClass() throws IOException
  {
    CacheConfig.setCacheDataDirPrefix(conf, TEST_CACHE_DIR_PREFIX);

    TestUtil.createCacheParentDirectories(conf, TEST_MAX_DISKS);
  }

  @BeforeMethod
  public void setUp()
  {
    CacheConfig.setCacheDataDirPrefix(conf, TEST_CACHE_DIR_PREFIX);
    CacheConfig.setBlockSize(conf, TEST_BLOCK_SIZE);
    CacheConfig.setCacheMetricsEnabled(conf, true);

    metrics = new MetricRegistry();
  }

  @AfterMethod
  public void tearDown()
  {
    conf.clear();
  }

  @AfterClass
  public void tearDownForClass() throws IOException
  {
    CacheConfig.setCacheDataDirPrefix(conf, TEST_CACHE_DIR_PREFIX);

    TestUtil.removeCacheParentDirectories(conf, TEST_MAX_DISKS);
  }

  /**
   * Verify that a JMX reporter is correctly registered when the configuration option is set.
   *
   * @throws IOException if an I/O error occurs while closing a reporter.
   */
  @Test
  public void testInitializeReporters_initializeJMX() throws IOException
  {
    CacheConfig.setMetricsReporters(conf, MetricsReporter.JMX.name());

    try (final BookKeeperMetrics bookKeeperMetrics = new BookKeeperMetrics(conf, metrics)) {
      assertTrue(containsReporterType(bookKeeperMetrics.reporters, JmxReporter.class));
    }
  }

  /**
   * Verify that a StatsD reporter is correctly registered when the configuration option is set.
   *
   * @throws IOException if an I/O error occurs while closing a reporter.
   */
  @Test
  public void testInitializeReporters_initializeStatsD() throws IOException
  {
    CacheConfig.setMetricsReporters(conf, MetricsReporter.STATSD.name());

    try (final BookKeeperMetrics bookKeeperMetrics = new BookKeeperMetrics(conf, metrics)) {
      assertTrue(containsReporterType(bookKeeperMetrics.reporters, StatsDReporter.class));
    }
  }

  /**
   * Verify that a Ganglia reporter is correctly registered when the configuration option is set.
   *
   * @throws IOException if an I/O error occurs while closing a reporter.
   */
  @Test
  public void testInitializeReporters_initializeGanglia() throws IOException
  {
    CacheConfig.setMetricsReporters(conf, MetricsReporter.GANGLIA.name());

    try (final BookKeeperMetrics bookKeeperMetrics = new BookKeeperMetrics(conf, metrics)) {
      assertTrue(containsReporterType(bookKeeperMetrics.reporters, GangliaReporter.class));
    }
  }

  /**
   * Verify that both JMX and StatsD reporters are correctly registered when the configuration option is set.
   *
   * @throws IOException if an I/O error occurs while closing a reporter.
   */
  @Test
  public void testInitializeReporters_initializeJMXAndStatsD() throws IOException
  {
    CacheConfig.setMetricsReporters(conf, Joiner.on(",").join(MetricsReporter.STATSD.name(), MetricsReporter.JMX.name()));

    try (final BookKeeperMetrics bookKeeperMetrics = new BookKeeperMetrics(conf, metrics)) {
      assertTrue(containsReporterType(bookKeeperMetrics.reporters, StatsDReporter.class));
      assertTrue(containsReporterType(bookKeeperMetrics.reporters, JmxReporter.class));
    }
  }

  /**
   * Verify that no reporters are registered when the configuration option is set.
   *
   * @throws IOException if an I/O error occurs while closing a reporter.
   */
  @Test
  public void testInitializeReporters_noneInitialized() throws IOException
  {
    CacheConfig.setMetricsReporters(conf, "");

    try (final BookKeeperMetrics bookKeeperMetrics = new BookKeeperMetrics(conf, metrics)) {
      assertFalse(containsReporterType(bookKeeperMetrics.reporters, StatsDReporter.class));
      assertFalse(containsReporterType(bookKeeperMetrics.reporters, JmxReporter.class));
    }
  }

  /**
   * Verify that the collection of liveness metrics correctly returns all expected metrics.
   */
  @Test
  public void testLivenessMetricsGetAllNames()
  {
    Set<String> livenessMetricsNames = Sets.newHashSet(BookKeeperMetrics.LivenessMetric.METRIC_BOOKKEEPER_LIVE_WORKER_GAUGE.getMetricName());

    assertEquals(livenessMetricsNames, BookKeeperMetrics.LivenessMetric.getAllNames());
  }

  /**
   * Verify that the collection of cache metrics correctly returns all expected metrics.
   */
  @Test
  public void testCacheMetricsGetAllNames()
  {
    Set<String> cacheMetricsNames = Sets.newHashSet(
        BookKeeperMetrics.CacheMetric.METRIC_BOOKKEEPER_CACHE_EVICTION_COUNT.getMetricName(),
        BookKeeperMetrics.CacheMetric.METRIC_BOOKKEEPER_CACHE_INVALIDATION_COUNT.getMetricName(),
        BookKeeperMetrics.CacheMetric.METRIC_BOOKKEEPER_CACHE_EXPIRY_COUNT.getMetricName(),
        BookKeeperMetrics.CacheMetric.METRIC_BOOKKEEPER_CACHE_HIT_RATE_GAUGE.getMetricName(),
        BookKeeperMetrics.CacheMetric.METRIC_BOOKKEEPER_CACHE_MISS_RATE_GAUGE.getMetricName(),
        BookKeeperMetrics.CacheMetric.METRIC_BOOKKEEPER_CACHE_SIZE_GAUGE.getMetricName(),
        BookKeeperMetrics.CacheMetric.METRIC_BOOKKEEPER_TOTAL_REQUEST_COUNT.getMetricName(),
        BookKeeperMetrics.CacheMetric.METRIC_BOOKKEEPER_CACHE_REQUEST_COUNT.getMetricName(),
        BookKeeperMetrics.CacheMetric.METRIC_BOOKKEEPER_NONLOCAL_REQUEST_COUNT.getMetricName(),
        BookKeeperMetrics.CacheMetric.METRIC_BOOKKEEPER_REMOTE_REQUEST_COUNT.getMetricName());

    assertEquals(cacheMetricsNames, BookKeeperMetrics.CacheMetric.getAllNames());
  }

  /**
   * Verify that the collection of BookKeeper JVM metrics correctly returns all expected metrics.
   */
  @Test
  public void testBookKeeperJvmMetricsGetAllNames()
  {
    Set<String> cacheMetricsNames = Sets.newHashSet(
        BookKeeperMetrics.BookKeeperJvmMetric.METRIC_BOOKKEEPER_JVM_GC_PREFIX.getMetricName(),
        BookKeeperMetrics.BookKeeperJvmMetric.METRIC_BOOKKEEPER_JVM_THREADS_PREFIX.getMetricName(),
        BookKeeperMetrics.BookKeeperJvmMetric.METRIC_BOOKKEEPER_JVM_MEMORY_PREFIX.getMetricName());

    assertEquals(cacheMetricsNames, BookKeeperMetrics.BookKeeperJvmMetric.getAllNames());
  }

  /**
   * Verify that the collection of LocalDataTransferServer JVM metrics correctly returns all expected metrics.
   */
  @Test
  public void testLDTSJvmMetricsGetAllNames()
  {
    Set<String> cacheMetricsNames = Sets.newHashSet(
        BookKeeperMetrics.LDTSJvmMetric.METRIC_LDTS_JVM_GC_PREFIX.getMetricName(),
        BookKeeperMetrics.LDTSJvmMetric.METRIC_LDTS_JVM_THREADS_PREFIX.getMetricName(),
        BookKeeperMetrics.LDTSJvmMetric.METRIC_LDTS_JVM_MEMORY_PREFIX.getMetricName());

    assertEquals(cacheMetricsNames, BookKeeperMetrics.LDTSJvmMetric.getAllNames());
  }

  /**
   * Checks if the provided reporter collection contains an instance of the desired reporter type.
   *
   * @param reporters       The reporter collection to check.
   * @param reporterClass   The reporter type to check for.
   * @return true if the collection contains the desired type, false otherwise.
   */
  private boolean containsReporterType(Collection<Closeable> reporters, Class<?> reporterClass)
  {
    for (Closeable reporter : reporters) {
      if (reporterClass.isInstance(reporter)) {
        return true;
      }
    }
    return false;
  }
}
