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

import com.codahale.metrics.MetricRegistry;
import com.qubole.rubix.common.metrics.BookKeeperMetrics;
import com.qubole.rubix.common.metrics.MetricsReporter;
import com.qubole.rubix.spi.CacheConfig;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import java.io.FileNotFoundException;
import java.lang.management.ManagementFactory;
import java.util.HashSet;
import java.util.Set;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class BaseServerTest
{
  protected enum ServerType
  {
    BOOKKEEPER,
    MOCK_BOOKKEEPER,
    LOCAL_DATA_TRANSFER_SERVER
  }

  private static BookKeeperServer bookKeeperServer;
  private MockBookKeeperServer mockBookKeeperServer;

  private static final Log log = LogFactory.getLog(BaseServerTest.class);
  protected static final String JMX_METRIC_NAME_PATTERN = "metrics:*";

  /**
   * Verify the behavior of the cache metrics for a given server type.
   *
   * @param serverType        The type of server to test against.
   * @param conf              The current Hadoop configuration.
   * @param metrics           The metrics registry to check against.
   * @param areMetricsEnabled Whether the metrics should be registered when the server is run.
   * @throws InterruptedException if the current thread is interrupted while sleeping.
   */
  protected void testCacheMetrics(ServerType serverType, Configuration conf, MetricRegistry metrics, boolean areMetricsEnabled) throws InterruptedException, MalformedObjectNameException
  {
    CacheConfig.setCacheMetricsEnabled(conf, areMetricsEnabled);
    CacheConfig.setOnMaster(conf, true);

    Set<String> metricsToVerify;
    switch (serverType) {
      case LOCAL_DATA_TRANSFER_SERVER:
        throw new IllegalArgumentException("No cache metrics available for LocalDataTransferServer");
      case BOOKKEEPER:
      case MOCK_BOOKKEEPER:
        metricsToVerify = BookKeeperMetrics.CacheMetric.getAllNames();
        break;
      default:
        throw new IllegalArgumentException("Invalid server type " + serverType.name());
    }

    checkMetrics(serverType, conf, metrics, metricsToVerify, areMetricsEnabled, false);
  }

  /**
   * Verify the behavior of the health metrics for a given server type.
   *
   * @param serverType        The type of server to test against.
   * @param conf              The current Hadoop configuration.
   * @param metrics           The metrics registry to check against.
   * @param areMetricsEnabled Whether the metrics should be registered when the server is run.
   * @throws InterruptedException if the current thread is interrupted while sleeping.
   */
  protected void testHealthMetrics(ServerType serverType, Configuration conf, MetricRegistry metrics, boolean areMetricsEnabled) throws InterruptedException, MalformedObjectNameException
  {
    CacheConfig.setHealthMetricsEnabled(conf, areMetricsEnabled);
    CacheConfig.setOnMaster(conf, true);

    Set<String> metricsToVerify;
    switch (serverType) {
      case LOCAL_DATA_TRANSFER_SERVER:
        throw new IllegalArgumentException("No health metrics available for LocalDataTransferServer");
      case BOOKKEEPER:
      case MOCK_BOOKKEEPER:
        metricsToVerify = BookKeeperMetrics.HealthMetric.getAllNames();
        break;
      default:
        throw new IllegalArgumentException("Invalid server type " + serverType.name());
    }

    checkMetrics(serverType, conf, metrics, metricsToVerify, areMetricsEnabled, false);
  }

  /**
   * Verify the behavior of the JVM metrics for a given server type.
   *
   * @param serverType        The type of server to test against.
   * @param conf              The current Hadoop configuration.
   * @param metrics           The metrics registry to check against.
   * @param areMetricsEnabled Whether the metrics should be registered when the server is run.
   * @throws InterruptedException if the current thread is interrupted while sleeping.
   */
  protected void testJvmMetrics(ServerType serverType, Configuration conf, MetricRegistry metrics, boolean areMetricsEnabled) throws InterruptedException, MalformedObjectNameException
  {
    CacheConfig.setJvmMetricsEnabled(conf, areMetricsEnabled);
    CacheConfig.setOnMaster(conf, true);

    Set<String> metricsToVerify;
    switch (serverType) {
      case LOCAL_DATA_TRANSFER_SERVER:
        metricsToVerify = BookKeeperMetrics.LDTSJvmMetric.getAllNames();
        break;
      case BOOKKEEPER:
      case MOCK_BOOKKEEPER:
        metricsToVerify = BookKeeperMetrics.BookKeeperJvmMetric.getAllNames();
        break;
      default:
        throw new IllegalArgumentException("Invalid server type " + serverType.name());
    }

    checkMetrics(serverType, conf, metrics, metricsToVerify, areMetricsEnabled, true);
  }

  /**
   * Verify that all registered metrics are removed once the server has stopped.
   */
  protected void verifyMetricsAreRemoved(ServerType serverType, Configuration conf, MetricRegistry metrics) throws InterruptedException
  {
    CacheConfig.setJvmMetricsEnabled(conf, true);
    CacheConfig.setOnMaster(conf, true);

    assertTrue(metrics.getNames().size() == 0, "Metrics should not be registered before server is started.");

    startServer(serverType, conf, metrics);

    assertTrue(metrics.getNames().size() > 0, "Metrics should be registered once server is started.");

    stopServer(serverType);

    assertTrue(metrics.getNames().size() == 0, "Metrics should not be registered after server has stopped.");
  }

  /**
   * Get the set of names for metrics registered to JMX.
   *
   * @return The set of names for all registered metrics.
   * @throws MalformedObjectNameException if the format of the pattern string does not correspond to a valid ObjectName.
   */
  protected Set<String> getJmxMetricsNames() throws MalformedObjectNameException
  {
    Set<String> registeredMetricsNames = new HashSet<>();
    for (ObjectName metricObjectName : ManagementFactory.getPlatformMBeanServer().queryNames(new ObjectName(JMX_METRIC_NAME_PATTERN), null)) {
      registeredMetricsNames.add(metricObjectName.getKeyProperty("name"));
    }
    return registeredMetricsNames;
  }

  /**
   * Start an instance of the BookKeeper server.
   *
   * @param conf    The current Hadoop configuration.
   * @param metrics The current metrics registry.
   * @throws InterruptedException if the current thread is interrupted while sleeping.
   */
  protected static void startBookKeeperServer(final Configuration conf, final MetricRegistry metrics) throws InterruptedException
  {
    if (bookKeeperServer != null) {
      throw new IllegalStateException("A BookKeeperServer is already running");
    }

    bookKeeperServer = new BookKeeperServer();
    final Thread thread = new Thread()
    {
      public void run()
      {
        bookKeeperServer.startServer(conf, metrics);
      }
    };
    thread.start();

    while (!bookKeeperServer.isServerUp()) {
      Thread.sleep(200);
      log.info("Waiting for BookKeeper Server to come up");
    }
  }

  /**
   * Start an instance of the BookKeeper server with an initial delay.
   *
   * @param conf          The current Hadoop configuration.
   * @param metrics       The current metrics registry.
   * @param initialDelay  The delay before starting the server. (ms)
   */
  protected static void startBookKeeperServerWithDelay(final Configuration conf, final MetricRegistry metrics, final int initialDelay)
  {
    if (bookKeeperServer != null) {
      throw new IllegalStateException("A BookKeeperServer is already running");
    }
    bookKeeperServer = new BookKeeperServer();

    final Thread thread = new Thread()
    {
      public void run()
      {
        try {
          Thread.sleep(initialDelay);
        }
        catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        bookKeeperServer.startServer(conf, metrics);
      }
    };
    thread.start();
  }

  /**
   * Stop the currently running BookKeeper server instance.
   */
  protected static void stopBookKeeperServer()
  {
    if (bookKeeperServer != null) {
      bookKeeperServer.stopServer();
      bookKeeperServer = null;
    }
    else {
      throw new IllegalStateException("BookKeeperServer hasn't been started yet");
    }
  }

  /**
   * Start an instance of the local data transfer server.
   *
   * @param conf    The current Hadoop configuration.
   * @param metrics The current metrics registry.
   * @throws InterruptedException if the current thread is interrupted while sleeping.
   */
  protected void startLocalDataTransferServer(final Configuration conf, final MetricRegistry metrics) throws InterruptedException
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
   * Stop the currently running local data transfer server instance.
   */
  protected void stopLocalDataTransferServer()
  {
    LocalDataTransferServer.stopServer();
  }

  /**
   * Start an instance of the mock BookKeeper server.
   *
   * @param conf    The current Hadoop configuration.
   * @param metrics The current metrics registry.
   */
  protected void startMockBookKeeperServer(final Configuration conf, final MetricRegistry metrics)
  {
    if (mockBookKeeperServer != null) {
      throw new IllegalStateException("A MockBookKeeperServer is already running");
    }

    mockBookKeeperServer = new MockBookKeeperServer();
    mockBookKeeperServer.startServer(conf, metrics);
  }

  /**
   * Stop the currently running mock BookKeeper server instance.
   */
  protected void stopMockBookKeeperServer()
  {
    if (mockBookKeeperServer != null) {
      mockBookKeeperServer.stopServer();
      mockBookKeeperServer = null;
    }
    else {
      throw new IllegalStateException("MockBookKeeperServer hasn't been started yet");
    }
  }

  /**
   * Verify the behavior of the metrics registry.
   *
   * @param serverType        The type of server to test against.
   * @param conf              The current Hadoop configuration.
   * @param metrics           The metrics registry to check against.
   * @param metricsToVerify   The metrics to verify.
   * @param areMetricsEnabled Whether the metrics should be registered when the server is run.
   * @param usePartialMatch   Whether to use a partial match when comparing metrics names.
   * @throws InterruptedException if the current thread is interrupted while sleeping.
   */
  private void checkMetrics(ServerType serverType, Configuration conf, MetricRegistry metrics, Set<String> metricsToVerify, boolean areMetricsEnabled, boolean usePartialMatch) throws InterruptedException, MalformedObjectNameException
  {
    CacheConfig.setMetricsReporters(conf, MetricsReporter.JMX.name());

    Set<String> metricsNames = getJmxMetricsNames();
    assertDoesNotContainMetrics(metricsNames, metricsToVerify, usePartialMatch);

    startServer(serverType, conf, metrics);

    try {
      metricsNames = getJmxMetricsNames();
      if (areMetricsEnabled) {
        assertContainsMetrics(metricsNames, metricsToVerify, usePartialMatch);
      }
      else {
        assertDoesNotContainMetrics(metricsNames, metricsToVerify, usePartialMatch);
      }
    }
    finally {
      stopServer(serverType);
    }

    metricsNames = getJmxMetricsNames();
    assertDoesNotContainMetrics(metricsNames, metricsToVerify, usePartialMatch);
  }

  /**
   * Start an instance of the given server type.
   *
   * @param serverType  The type of server to start
   * @param conf    The current Hadoop configuration.
   * @param metrics The current metrics registry.
   * @throws InterruptedException if the current thread is interrupted while sleeping.
   */
  private void startServer(ServerType serverType, Configuration conf, MetricRegistry metrics) throws InterruptedException
  {
    switch (serverType) {
      case BOOKKEEPER:
        startBookKeeperServer(conf, metrics);
        break;
      case MOCK_BOOKKEEPER:
        startMockBookKeeperServer(conf, metrics);
        break;
      case LOCAL_DATA_TRANSFER_SERVER:
        startLocalDataTransferServer(conf, metrics);
        break;
    }
  }

  /**
   * Stop an instance of the given server type.
   *
   * @param serverType  The type of server to start
   */
  private void stopServer(ServerType serverType)
  {
    switch (serverType) {
      case BOOKKEEPER:
        stopBookKeeperServer();
        break;
      case MOCK_BOOKKEEPER:
        stopMockBookKeeperServer();
        break;
      case LOCAL_DATA_TRANSFER_SERVER:
        stopLocalDataTransferServer();
        break;
    }
  }

  /**
   * Run assertion checks to verify that the set of metrics names contains the provided metrics.
   *
   * @param metricsNames    The set of metrics names to check.
   * @param metricsToVerify The metrics to verify.
   * @param usePartialMatch Whether to use a partial match when comparing metrics names.
   */
  private void assertContainsMetrics(Set<String> metricsNames, Set<String> metricsToVerify, boolean usePartialMatch)
  {
    for (String metric : metricsToVerify) {
      if (usePartialMatch) {
        assertTrue(doesSetContainPartialMatch(metricsNames, metric), String.format("%s metrics set should be registered.", metric));
      }
      else {
        assertTrue(metricsNames.contains(metric), String.format("%s should be registered.", metric));
      }
    }
  }

  /**
   * Run assertion checks to verify that the set of metrics names does not contain the provided metrics.
   *
   * @param metricsNames    The set of metrics names to check.
   * @param metricsToVerify The metrics to verify.
   * @param usePartialMatch Whether to use a partial match when comparing metrics names.
   */
  private void assertDoesNotContainMetrics(Set<String> metricsNames, Set<String> metricsToVerify, boolean usePartialMatch)
  {
    for (String metric : metricsToVerify) {
      if (usePartialMatch) {
        assertFalse(doesSetContainPartialMatch(metricsNames, metric), String.format("%s metrics set should not be registered.", metric));
      }
      else {
        assertFalse(metricsNames.contains(metric), String.format("%s should not be registered.", metric));
      }
    }
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

  /**
   * Class to mock the behaviour of {@link BookKeeperServer} for testing registering & reporting metrics.
   */
  private static class MockBookKeeperServer extends BookKeeperServer
  {
    public void startServer(Configuration conf, MetricRegistry metricRegistry)
    {
      metrics = metricRegistry;
      try {
        // Initializing this BookKeeper here allows it to register the live worker count metric for testing.
        new CoordinatorBookKeeper(conf, metrics);
      }
      catch (FileNotFoundException e) {
        log.error("Cache directories could not be created", e);
        return;
      }
      registerMetrics(conf);
    }

    public void stopServer()
    {
      removeMetrics();
    }
  }
}
