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

import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.qubole.rubix.bookkeeper.metrics.BookKeeperMetrics;
import com.qubole.rubix.spi.CacheConfig;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Set;
import java.util.SortedSet;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestLocalDataTransferServer
{
  private static final Log log = LogFactory.getLog(TestLocalDataTransferServer.class);

  private final Configuration conf = new Configuration();
  private final MetricRegistry metrics = new MetricRegistry();

  @BeforeMethod
  public void setUp()
  {
  }

  @AfterMethod
  public void tearDown()
  {
    conf.clear();
    metrics.removeMatching(MetricFilter.ALL);
  }

  /**
   * Verify that JVM metrics are registered when configured to.
   *
   * @throws InterruptedException if the current thread is interrupted while sleeping.
   */
  @Test
  public void testJvmMetricsEnabled() throws InterruptedException
  {
    CacheConfig.setJvmMetricsEnabled(conf, true);
    CacheConfig.setOnMaster(conf, true);

    SortedSet<String> metricsNames = metrics.getNames();
    assertFalse(doesSetContainPartialMatch(metricsNames, BookKeeperMetrics.METRIC_LDTS_JVM_GC_PREFIX), "GC metrics should not be registered before server is started.");
    assertFalse(doesSetContainPartialMatch(metricsNames, BookKeeperMetrics.METRIC_LDTS_JVM_THREADS_PREFIX), "Thread metrics should not be registered before server is started.");
    assertFalse(doesSetContainPartialMatch(metricsNames, BookKeeperMetrics.METRIC_LDTS_JVM_MEMORY_PREFIX), "Memory metrics should not be registered before server is started.");

    startLocalDataTransferServer();

    metricsNames = metrics.getNames();
    assertTrue(doesSetContainPartialMatch(metricsNames, BookKeeperMetrics.METRIC_LDTS_JVM_GC_PREFIX), "GC metrics should be registered after server is started.");
    assertTrue(doesSetContainPartialMatch(metricsNames, BookKeeperMetrics.METRIC_LDTS_JVM_THREADS_PREFIX), "Thread metrics should be registered after server is started.");
    assertTrue(doesSetContainPartialMatch(metricsNames, BookKeeperMetrics.METRIC_LDTS_JVM_MEMORY_PREFIX), "Memory metrics should be registered after server is started.");

    stopLocalDataTransferServer();

    metricsNames = metrics.getNames();
    assertFalse(doesSetContainPartialMatch(metricsNames, BookKeeperMetrics.METRIC_LDTS_JVM_GC_PREFIX), "GC metrics should not be registered after server is stopped.");
    assertFalse(doesSetContainPartialMatch(metricsNames, BookKeeperMetrics.METRIC_LDTS_JVM_THREADS_PREFIX), "Thread metrics should not be registered after server is stopped.");
    assertFalse(doesSetContainPartialMatch(metricsNames, BookKeeperMetrics.METRIC_LDTS_JVM_MEMORY_PREFIX), "Memory metrics should not be registered after server is stopped.");
  }

  /**
   * Verify that JVM metrics are not registered when configured not to.
   *
   * @throws InterruptedException if the current thread is interrupted while sleeping.
   */
  @Test
  public void testJvmMetricsNotEnabled() throws InterruptedException
  {
    CacheConfig.setJvmMetricsEnabled(conf, false);
    CacheConfig.setOnMaster(conf, true);

    SortedSet<String> metricsNames = metrics.getNames();
    assertFalse(doesSetContainPartialMatch(metricsNames, BookKeeperMetrics.METRIC_LDTS_JVM_GC_PREFIX), "GC metrics should not be registered before server is started.");
    assertFalse(doesSetContainPartialMatch(metricsNames, BookKeeperMetrics.METRIC_LDTS_JVM_THREADS_PREFIX), "Thread metrics should not be registered before server is started.");
    assertFalse(doesSetContainPartialMatch(metricsNames, BookKeeperMetrics.METRIC_LDTS_JVM_MEMORY_PREFIX), "Memory metrics should not be registered before server is started.");

    startLocalDataTransferServer();

    metricsNames = metrics.getNames();
    assertFalse(doesSetContainPartialMatch(metricsNames, BookKeeperMetrics.METRIC_LDTS_JVM_GC_PREFIX), "GC metrics should not be registered after server is started.");
    assertFalse(doesSetContainPartialMatch(metricsNames, BookKeeperMetrics.METRIC_LDTS_JVM_THREADS_PREFIX), "Thread metrics should not be registered after server is started.");
    assertFalse(doesSetContainPartialMatch(metricsNames, BookKeeperMetrics.METRIC_LDTS_JVM_MEMORY_PREFIX), "Memory metrics should not be registered after server is started.");

    stopLocalDataTransferServer();

    metricsNames = metrics.getNames();
    assertFalse(doesSetContainPartialMatch(metricsNames, BookKeeperMetrics.METRIC_LDTS_JVM_GC_PREFIX), "GC metrics should not be registered after server is stopped.");
    assertFalse(doesSetContainPartialMatch(metricsNames, BookKeeperMetrics.METRIC_LDTS_JVM_THREADS_PREFIX), "Thread metrics should not be registered after server is stopped.");
    assertFalse(doesSetContainPartialMatch(metricsNames, BookKeeperMetrics.METRIC_LDTS_JVM_MEMORY_PREFIX), "Memory metrics should not be registered after server is stopped.");
  }

  /**
   * Verify that all registered metrics are removed once the Local Data Transfer Server has stopped.
   */
  @Test
  public void verifyMetricsAreRemoved() throws InterruptedException
  {
    CacheConfig.setJvmMetricsEnabled(conf, true);
    CacheConfig.setOnMaster(conf, true);

    assertTrue(metrics.getNames().size() == 0, "Metrics should not be registered before server is started.");

    startLocalDataTransferServer();

    assertTrue(metrics.getNames().size() > 0, "Metrics should be registered once server is started.");

    stopLocalDataTransferServer();

    assertTrue(metrics.getNames().size() == 0, "Metrics should not be registered after server has stopped.");
  }

  /**
   * Start an instance of the BookKeeper server.
   *
   * @throws InterruptedException if the current thread is interrupted while sleeping.
   */
  private void startLocalDataTransferServer() throws InterruptedException
  {
    final Thread thread = new Thread()
    {
      public void run()
      {
        LocalDataTransferServer.startServer(conf, metrics);
      }
    };
    thread.start();

    while (!LocalDataTransferServer.isServerUp()) {
      Thread.sleep(200);
      log.info("Waiting for LDTS to come up");
    }
  }

  /**
   * Stop the currently running BookKeeper server instance.
   */
  private void stopLocalDataTransferServer()
  {
    LocalDataTransferServer.stopServer();
  }

  /**
   * Check if a given set contains a partial match for the desired element.
   *
   * @param stringSet     The set to search.
   * @param searchString  The partial string to search for.
   * @return true if the set contains a partial match, false otherwise.
   */
  private boolean doesSetContainPartialMatch(Set<String> stringSet, String searchString)
  {
    for (String element : stringSet) {
      if (element.contains(searchString)) {
        return true;
      }
    }
    return false;
  }
}
