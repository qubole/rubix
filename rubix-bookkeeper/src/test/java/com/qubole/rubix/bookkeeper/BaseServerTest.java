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
package com.qubole.rubix.bookkeeper;

import com.codahale.metrics.MetricRegistry;
import com.qubole.rubix.common.metrics.BookKeeperMetrics;
import com.qubole.rubix.common.metrics.MetricsReporter;
import com.qubole.rubix.spi.BookKeeperFactory;
import com.qubole.rubix.spi.CacheConfig;
import com.qubole.rubix.spi.RetryingPooledBookkeeperClient;
import com.qubole.rubix.spi.fop.Poolable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.shaded.transport.TSocket;
import org.apache.thrift.shaded.transport.TTransport;
import org.apache.thrift.shaded.transport.TTransportException;
import org.mockito.ArgumentMatchers;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.HashSet;
import java.util.Set;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class BaseServerTest
{
  protected enum ServerType
  {
    COORDINATOR_BOOKKEEPER,
    MOCK_COORDINATOR_BOOKKEEPER,
    MOCK_WORKER_BOOKKEEPER,
    LOCAL_DATA_TRANSFER_SERVER
  }

  private static BookKeeperServer bookKeeperServer;
  private static MockCoordinatorBookKeeperServer mockCoordinatorBookKeeperServer;
  private static MockWorkerBookKeeperServer mockWorkerBookKeeperServer;

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
      case COORDINATOR_BOOKKEEPER:
      case MOCK_COORDINATOR_BOOKKEEPER:
      case MOCK_WORKER_BOOKKEEPER:
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
      case COORDINATOR_BOOKKEEPER:
      case MOCK_COORDINATOR_BOOKKEEPER:
        metricsToVerify = BookKeeperMetrics.HealthMetric.getAllNames();
        break;
      case MOCK_WORKER_BOOKKEEPER:
        throw new IllegalArgumentException("No health metrics available for WorkerBookKeeper");
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
      case COORDINATOR_BOOKKEEPER:
      case MOCK_COORDINATOR_BOOKKEEPER:
      case MOCK_WORKER_BOOKKEEPER:
        metricsToVerify = BookKeeperMetrics.BookKeeperJvmMetric.getAllNames();
        break;
      default:
        throw new IllegalArgumentException("Invalid server type " + serverType.name());
    }

    checkMetrics(serverType, conf, metrics, metricsToVerify, areMetricsEnabled, true);
  }

  /**
   * Verify the behavior of the validation metrics for a given server type.
   *
   * @param serverType        The type of server to test against.
   * @param conf              The current Hadoop configuration.
   * @param metrics           The metrics registry to check against.
   * @param areMetricsEnabled Whether the metrics should be registered when the server is run.
   * @throws InterruptedException if the current thread is interrupted while sleeping.
   */
  protected void testValidationMetrics(ServerType serverType, Configuration conf, MetricRegistry metrics, boolean areMetricsEnabled) throws MalformedObjectNameException, InterruptedException
  {
    CacheConfig.setValidationEnabled(conf, areMetricsEnabled);

    Set<String> metricsToVerify;
    switch (serverType) {
      case LOCAL_DATA_TRANSFER_SERVER:
        throw new IllegalArgumentException("No validation metrics available for LocalDataTransferServer");
      case COORDINATOR_BOOKKEEPER:
      case MOCK_COORDINATOR_BOOKKEEPER:
        throw new IllegalArgumentException("No validation metrics available for CoordinatorBookKeeper");
      case MOCK_WORKER_BOOKKEEPER:
        metricsToVerify = BookKeeperMetrics.ValidationMetric.getAllNames();
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
  protected static void startCoordinatorBookKeeperServer(final Configuration conf, final MetricRegistry metrics) throws InterruptedException
  {
    if (bookKeeperServer != null) {
      throw new IllegalStateException("A BookKeeperServer is already running");
    }

    CacheConfig.setOnMaster(conf, true);

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
  protected static void startCoordinatorBookKeeperServerWithDelay(final Configuration conf, final MetricRegistry metrics, final int initialDelay)
  {
    if (bookKeeperServer != null) {
      throw new IllegalStateException("A BookKeeperServer is already running");
    }

    CacheConfig.setOnMaster(conf, true);

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
   * Start an instance of the mock CoordinatorBookKeeper server.
   *
   * @param conf    The current Hadoop configuration.
   * @param metrics The current metrics registry.
   */
  protected void startMockCoordinatorBookKeeperServer(final Configuration conf, final MetricRegistry metrics) throws InterruptedException
  {
    if (mockCoordinatorBookKeeperServer != null) {
      throw new IllegalStateException("A MockCoordinatorBookKeeperServer is already running");
    }

    CacheConfig.setOnMaster(conf, true);

    mockCoordinatorBookKeeperServer = new MockCoordinatorBookKeeperServer();
    final Thread thread = new Thread()
    {
      public void run()
      {
        mockCoordinatorBookKeeperServer.startServer(conf, metrics);
      }
    };
    thread.start();

    while (!mockCoordinatorBookKeeperServer.isServerUp()) {
      Thread.sleep(200);
      log.info("Waiting for Mock CoordinatorBookKeeper Server to come up");
    }
  }

  /**
   * Stop the currently running MockCoordinatorBookKeeperServer instance.
   */
  protected void stopMockCoordinatorBookKeeperServer()
  {
    if (mockCoordinatorBookKeeperServer != null) {
      mockCoordinatorBookKeeperServer.stopServer();
      mockCoordinatorBookKeeperServer = null;
    }
    else {
      throw new IllegalStateException("MockCoordinatorBookKeeperServer hasn't been started yet");
    }
  }

  /**
   * Start an instance of the mock WorkerBookKeeper server.
   *
   * @param conf    The current Hadoop configuration.
   * @param metrics The current metrics registry.
   */
  protected void startMockWorkerBookKeeperServer(final Configuration conf, final MetricRegistry metrics) throws InterruptedException
  {
    if (mockWorkerBookKeeperServer != null) {
      throw new IllegalStateException("A MockWorkerBookKeeperServer is already running");
    }

    CacheConfig.setOnMaster(conf, false);

    mockWorkerBookKeeperServer = new MockWorkerBookKeeperServer();
    final Thread thread = new Thread()
    {
      public void run()
      {
        mockWorkerBookKeeperServer.startServer(conf, metrics);
      }
    };
    thread.start();

    while (!mockWorkerBookKeeperServer.isServerUp()) {
      Thread.sleep(200);
      log.info("Waiting for Mock WorkerBookKeeper Server to come up");
    }
  }

  /**
   * Stop the currently running MockWorkerBookKeeperServer instance.
   */
  protected void stopMockWorkerBookKeeperServer()
  {
    if (mockWorkerBookKeeperServer != null) {
      mockWorkerBookKeeperServer.stopServer();
      mockWorkerBookKeeperServer = null;
    }
    else {
      throw new IllegalStateException("MockWorkerBookKeeperServer hasn't been started yet");
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
      case COORDINATOR_BOOKKEEPER:
        startCoordinatorBookKeeperServer(conf, metrics);
        break;
      case MOCK_COORDINATOR_BOOKKEEPER:
        startMockCoordinatorBookKeeperServer(conf, metrics);
        break;
      case MOCK_WORKER_BOOKKEEPER:
        startMockWorkerBookKeeperServer(conf, metrics);
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
      case COORDINATOR_BOOKKEEPER:
        stopBookKeeperServer();
        break;
      case MOCK_COORDINATOR_BOOKKEEPER:
        stopMockCoordinatorBookKeeperServer();
        break;
      case MOCK_WORKER_BOOKKEEPER:
        stopMockWorkerBookKeeperServer();
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
  private static class MockCoordinatorBookKeeperServer extends BookKeeperServer
  {
    private boolean isServerUp;

    public BookKeeper startServer(Configuration conf, MetricRegistry metricRegistry)
    {
      BookKeeper bookKeeper;
      metrics = metricRegistry;
      bookKeeperMetrics = new BookKeeperMetrics(conf, metrics);
      try {
        // Initializing this BookKeeper here allows it to register the live worker count metric for testing.
        bookKeeper = new CoordinatorBookKeeper(conf, bookKeeperMetrics);
      }
      catch (FileNotFoundException e) {
        log.error("Cache directories could not be created", e);
        return null;
      }
      registerMetrics(conf);
      isServerUp = true;
      return bookKeeper;
    }

    public void stopServer()
    {
      removeMetrics();
      try {
        bookKeeperMetrics.close();
      }
      catch (IOException e) {
        log.error("Metrics reporters could not be closed", e);
      }
      isServerUp = false;
    }

    public boolean isServerUp()
    {
      return isServerUp;
    }
  }

  /**
   * Class to mock the behaviour of {@link BookKeeperServer} for testing registering & reporting metrics.
   */
  private static class MockWorkerBookKeeperServer extends BookKeeperServer
  {
    private boolean isServerUp;

    public BookKeeper startServer(Configuration conf, MetricRegistry metricRegistry)
    {
      BookKeeper bookKeeper = null;
      final BookKeeperFactory bookKeeperFactory = mock(BookKeeperFactory.class);
      try {
        when(bookKeeperFactory.createBookKeeperClient(anyString(), ArgumentMatchers.<Configuration>any())).thenReturn(
                new RetryingPooledBookkeeperClient(
                        new Poolable<TTransport>(new TSocket("localhost", CacheConfig.getBookKeeperServerPort(conf), CacheConfig.getServerConnectTimeout(conf)), null, "localhost"),
                        null,
                        conf));
      }
      catch (TTransportException e) {
        log.error("Error starting MockWorkerBookKeeperServer for test", e);
      }

      metrics = metricRegistry;
      bookKeeperMetrics = new BookKeeperMetrics(conf, metrics);
      try {
        bookKeeper = new WorkerBookKeeper(conf, bookKeeperMetrics, bookKeeperFactory);
      }
      catch (FileNotFoundException e) {
        log.error("Cache directories could not be created", e);
        return bookKeeper;
      }
      registerMetrics(conf);
      isServerUp = true;
      return bookKeeper;
    }

    public void stopServer()
    {
      removeMetrics();
      try {
        bookKeeperMetrics.close();
      }
      catch (IOException e) {
        log.error("Metrics reporters could not be closed", e);
      }
      isServerUp = false;
    }

    public boolean isServerUp()
    {
      return isServerUp;
    }
  }
}
