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
import com.google.common.base.Joiner;
import com.qubole.rubix.bookkeeper.BookKeeper;
import com.qubole.rubix.bookkeeper.CoordinatorBookKeeper;
import com.qubole.rubix.core.utils.DeleteFileVisitor;
import com.qubole.rubix.spi.CacheConfig;
import com.qubole.rubix.spi.ClusterType;
import com.readytalk.metrics.StatsDReporter;
import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.shaded.TException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestBookKeeperMetrics
{
  private static final String cacheTestDirPrefix = System.getProperty("java.io.tmpdir") + "/bookKeeperMetricsTest/";
  private static final int BLOCK_SIZE = 100;
  private static final int MAX_DISKS = 5;

  private MetricRegistry metrics;
  private final Configuration conf = new Configuration();

  private BookKeeper bookKeeper;

  @BeforeClass
  public void initializeCacheDirectories() throws IOException
  {
    CacheConfig.setMaxDisks(conf, MAX_DISKS);

    // Create cache directories
    Files.createDirectories(Paths.get(cacheTestDirPrefix));
    for (int i = 0; i < CacheConfig.getCacheMaxDisks(conf); i++) {
      Files.createDirectories(Paths.get(cacheTestDirPrefix, String.valueOf(i)));
    }
  }

  @BeforeMethod
  public void setUp() throws FileNotFoundException
  {
    metrics = new MetricRegistry();
    conf.clear();

    // Set configuration values for testing
    CacheConfig.setCacheDataDirPrefix(conf, cacheTestDirPrefix);
    CacheConfig.setMaxDisks(conf, MAX_DISKS);
    CacheConfig.setBlockSize(conf, BLOCK_SIZE);

    bookKeeper = new CoordinatorBookKeeper(conf, metrics);
  }

  @AfterClass
  public void cleanUpCacheDirectories() throws IOException
  {
    Files.walkFileTree(Paths.get(cacheTestDirPrefix), new DeleteFileVisitor());
    Files.deleteIfExists(Paths.get(cacheTestDirPrefix));
  }

  /**
   * Verify that the metric representing total block hits is correctly registered & incremented.
   *
   * @throws TException when file metadata cannot be fetched or refreshed.
   */
  @Test
  public void verifyBlockHitsMetricIsReported() throws TException
  {
    final String remotePath = "/tmp/testPath";
    final long lastModified = 1514764800; // 2018-01-01T00:00:00
    final long fileLength = 5000;
    final long startBlock = 20;
    final long endBlock = 23;
    final long totalRequests = endBlock - startBlock;

    assertEquals(metrics.getCounters().get(BookKeeper.METRIC_BOOKKEEPER_LOCAL_CACHE_COUNT).getCount(), 0);
    bookKeeper.getCacheStatus(remotePath, fileLength, lastModified, startBlock, endBlock, ClusterType.TEST_CLUSTER_MANAGER.ordinal());
    assertEquals(metrics.getCounters().get(BookKeeper.METRIC_BOOKKEEPER_LOCAL_CACHE_COUNT).getCount(), totalRequests);
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

    final BookKeeperMetrics bookKeeperMetrics = new BookKeeperMetrics(conf, metrics);

    assertTrue(containsReporterType(bookKeeperMetrics.reporters, JmxReporter.class));

    bookKeeperMetrics.closeReporters();
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

    final BookKeeperMetrics bookKeeperMetrics = new BookKeeperMetrics(conf, metrics);

    assertTrue(containsReporterType(bookKeeperMetrics.reporters, StatsDReporter.class));

    bookKeeperMetrics.closeReporters();
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

    final BookKeeperMetrics bookKeeperMetrics = new BookKeeperMetrics(conf, metrics);

    assertTrue(containsReporterType(bookKeeperMetrics.reporters, StatsDReporter.class));
    assertTrue(containsReporterType(bookKeeperMetrics.reporters, JmxReporter.class));

    bookKeeperMetrics.closeReporters();
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

    final BookKeeperMetrics bookKeeperMetrics = new BookKeeperMetrics(conf, metrics);

    assertFalse(containsReporterType(bookKeeperMetrics.reporters, StatsDReporter.class));
    assertFalse(containsReporterType(bookKeeperMetrics.reporters, JmxReporter.class));

    bookKeeperMetrics.closeReporters();
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
