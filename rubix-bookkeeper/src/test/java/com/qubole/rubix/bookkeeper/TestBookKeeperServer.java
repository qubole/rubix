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
import com.qubole.rubix.common.metrics.MetricsReporter;
import com.qubole.rubix.common.utils.TestUtil;
import com.qubole.rubix.spi.CacheConfig;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.management.MalformedObjectNameException;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.Set;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestBookKeeperServer extends BaseServerTest
{
  private static final Log log = LogFactory.getLog(TestBookKeeperServer.class);

  private static final String TEST_CACHE_DIR_PREFIX = TestUtil.getTestCacheDirPrefix("TestBookKeeperServer");
  private static final int TEST_MAX_DISKS = 1;
  private static final int TEST_PACKET_SIZE = 32;
  private static final int TEST_SOCKET_TIMEOUT = 5000;

  private final Configuration conf = new Configuration();
  private MetricRegistry metrics = new MetricRegistry();

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
    CacheConfig.setMaxDisks(conf, TEST_MAX_DISKS);

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
   * Verify that health metrics are registered when configured to.
   */
  @Test
  public void testHealthMetricsEnabled() throws InterruptedException, MalformedObjectNameException
  {
    CacheConfig.setValidationEnabled(conf, true);
    super.testHealthMetrics(ServerType.COORDINATOR_BOOKKEEPER, conf, metrics, true);
  }

  /**
   * Verify that health metrics are not registered when configured not to.
   */
  @Test
  public void testHealthMetricsNotEnabled() throws InterruptedException, MalformedObjectNameException
  {
    super.testHealthMetrics(ServerType.COORDINATOR_BOOKKEEPER, conf, metrics, false);
  }

  /**
   * Verify that cache metrics are registered when configured to.
   */
  @Test
  public void testCacheMetricsEnabled() throws InterruptedException, MalformedObjectNameException
  {
    super.testCacheMetrics(ServerType.COORDINATOR_BOOKKEEPER, conf, metrics, true);
  }

  /**
   * Verify that cache metrics are not registered when configured not to.
   */
  @Test
  public void testCacheMetricsNotEnabled() throws InterruptedException, MalformedObjectNameException
  {
    super.testCacheMetrics(ServerType.COORDINATOR_BOOKKEEPER, conf, metrics, false);
  }

  /**
   * Verify that JVM metrics are registered when configured to.
   */
  @Test
  public void testJvmMetricsEnabled() throws InterruptedException, MalformedObjectNameException
  {
    super.testJvmMetrics(ServerType.COORDINATOR_BOOKKEEPER, conf, metrics, true);
  }

  /**
   * Verify that JVM metrics are not registered when configured not to.
   */
  @Test
  public void testJvmMetricsNotEnabled() throws InterruptedException, MalformedObjectNameException
  {
    super.testJvmMetrics(ServerType.COORDINATOR_BOOKKEEPER, conf, metrics, false);
  }

  /**
   * Verify that validation metrics are registered when configured to.
   */
  @Test
  public void testValidationMetricsEnabled() throws Exception
  {
    CacheConfig.setMetricsReporters(conf, "");
    startServer(ServerType.COORDINATOR_BOOKKEEPER, conf, new MetricRegistry());
    super.testValidationMetrics(ServerType.MOCK_WORKER_BOOKKEEPER, conf, metrics, true);
    stopBookKeeperServer();
  }

  /**
   * Verify that validation metrics are not registered when configured not to.
   */
  @Test
  public void testValidationMetricsNotEnabled() throws InterruptedException, MalformedObjectNameException
  {
    CacheConfig.setMetricsReporters(conf, "");
    startServer(ServerType.COORDINATOR_BOOKKEEPER, conf, new MetricRegistry());
    super.testValidationMetrics(ServerType.MOCK_WORKER_BOOKKEEPER, conf, metrics, false);
    stopBookKeeperServer();
  }

  /**
   * Verify that all registered metrics are removed once the BookKeeper server has stopped.
   */
  @Test
  public void verifyMetricsAreRemoved() throws InterruptedException
  {
    super.verifyMetricsAreRemoved(ServerType.COORDINATOR_BOOKKEEPER, conf, metrics);
  }

  /**
   * Verify that StatsDReporter reports metrics to StatsD on a master node when configured to.
   *
   * @throws InterruptedException if the current thread is interrupted while sleeping.
   * @throws IOException if an I/O error occurs while waiting for a response.
   */
  @Test
  public void verifyStatsDReporterIsReporting_onMaster_reportOnMaster() throws InterruptedException, IOException
  {
    final int statsDPort = 6789;
    final int testCasePort = 5678;
    final boolean shouldReport = true;

    MockStatsDThread statsDThread = startServersForTestingStatsDReporterOnMaster(statsDPort, testCasePort, shouldReport);

    try {
      assertTrue(isStatsDReporterFiring(testCasePort), "BookKeeperServer is not reporting to StatsD");
    }
    finally {
      statsDThread.stopThread();
      stopMockCoordinatorBookKeeperServer();
    }
  }

  /**
   * Verify that StatsDReporter does not report metrics to StatsD on a master node when configured not to.
   *
   * @throws InterruptedException if the current thread is interrupted while sleeping.
   * @throws IOException if an I/O error occurs while waiting for a response.
   */
  @Test
  public void verifyStatsDReporterIsReporting_onMaster_doNotReportOnMaster() throws InterruptedException, IOException
  {
    final int statsDPort = 6790;
    final int testCasePort = 5679;
    final boolean shouldReport = false;

    MockStatsDThread statsDThread = startServersForTestingStatsDReporterOnMaster(statsDPort, testCasePort, shouldReport);

    try {
      assertFalse(isStatsDReporterFiring(testCasePort), "BookKeeperServer should not report to StatsD");
    }
    finally {
      statsDThread.stopThread();
      stopMockCoordinatorBookKeeperServer();
    }
  }

  /**
   * Verify that StatsDReporter reports metrics to StatsD on a worker node when configured to.
   *
   * @throws InterruptedException if the current thread is interrupted while sleeping.
   * @throws IOException if an I/O error occurs while waiting for a response.
   */
  @Test
  public void verifyStatsDReporterIsReporting_onWorker_reportOnWorker() throws InterruptedException, IOException
  {
    final int statsDPort = 6791;
    final int testCasePort = 5680;
    final boolean shouldReport = true;

    MockStatsDThread statsDThread = startServersForTestingStatsDReporterForWorker(statsDPort, testCasePort, shouldReport);

    try {
      assertTrue(isStatsDReporterFiring(testCasePort), "BookKeeperServer is not reporting to StatsD");
    }
    finally {
      statsDThread.stopThread();
      stopMockCoordinatorBookKeeperServer();
    }
  }

  /**
   * Verify that StatsDReporter does not report metrics to StatsD on a master node when configured not to.
   *
   * @throws InterruptedException if the current thread is interrupted while sleeping.
   * @throws IOException if an I/O error occurs while waiting for a response.
   */
  @Test
  public void verifyStatsDReporterIsReporting_onWorker_doNotReportOnWorker() throws InterruptedException, IOException
  {
    final int statsDPort = 6792;
    final int testCasePort = 5681;
    final boolean shouldReport = false;

    MockStatsDThread statsDThread = startServersForTestingStatsDReporterForWorker(statsDPort, testCasePort, shouldReport);

    try {
      assertFalse(isStatsDReporterFiring(testCasePort), "BookKeeperServer should not report to StatsD");
    }
    finally {
      statsDThread.stopThread();
      stopMockCoordinatorBookKeeperServer();
    }
  }

  /**
   * Verify that JmxReporter reports metrics to JMX when configured to.
   *
   * @throws MalformedObjectNameException if the format of the pattern string does not correspond to a valid ObjectName.
   * @throws InterruptedException if the current thread is interrupted while sleeping.
   */
  @Test
  public void verifyMetricsAreReportedToJMX() throws MalformedObjectNameException, InterruptedException
  {
    CacheConfig.setMetricsReporters(conf, MetricsReporter.JMX.name());

    Set<String> metricsNames = getJmxMetricsNames();
    assertTrue(metricsNames.size() == 0, "Metrics should not be registered with JMX before server starts.");

    startMockCoordinatorBookKeeperServer(conf, metrics);

    metricsNames = getJmxMetricsNames();
    assertTrue(metricsNames.size() > 0, "Metrics should be registered with JMX after server starts.");

    stopMockCoordinatorBookKeeperServer();

    metricsNames = getJmxMetricsNames();
    assertTrue(metricsNames.size() == 0, "Metrics should not be registered with JMX after server stops.");
  }

  /**
   * Verify that JmxReporter does not report metrics to JMX when configured not to.
   *
   * @throws MalformedObjectNameException if the format of the pattern string does not correspond to a valid ObjectName.
   * @throws InterruptedException if the current thread is interrupted while sleeping.
   */
  @Test
  public void verifyMetricsAreNotReportedToJMX() throws MalformedObjectNameException, InterruptedException
  {
    CacheConfig.setMetricsReporters(conf, "");

    startMockCoordinatorBookKeeperServer(conf, metrics);

    final Set<String> metricsNames = getJmxMetricsNames();
    assertTrue(metricsNames.size() == 0, "Metrics should not be registered with JMX.");

    stopMockCoordinatorBookKeeperServer();
  }

  /**
   * Start & configure the servers necessary for running & testing StatsDReporter on a master node.
   *
   * @param statsDPort The port to send StatsD metrics to.
   * @param testCasePort The port from which to receive responses from the mock StatsD server.
   * @param shouldReportMetrics Whether metrics should be reported.
   * @throws SocketException if the socket for the mock StatsD server could not be created or bound.
   * @throws InterruptedException if the current thread is interrupted while sleeping.
   */
  private MockStatsDThread startServersForTestingStatsDReporterOnMaster(int statsDPort, int testCasePort, boolean shouldReportMetrics) throws SocketException, InterruptedException
  {
    CacheConfig.setOnMaster(conf, true);
    if (shouldReportMetrics) {
      CacheConfig.setMetricsReporters(conf, MetricsReporter.STATSD.name());
    }

    return startServersForTestingStatsDReporter(statsDPort, testCasePort);
  }

  /**
   * Start & configure the servers necessary for running & testing StatsDReporter on a worker node.
   *
   * @param statsDPort The port to send StatsD metrics to.
   * @param testCasePort The port from which to receive responses from the mock StatsD server.
   * @param shouldReportMetrics Whether metrics should be reported.
   * @throws SocketException if the socket for the mock StatsD server could not be created or bound.
   * @throws InterruptedException if the current thread is interrupted while sleeping.
   */
  private MockStatsDThread startServersForTestingStatsDReporterForWorker(int statsDPort, int testCasePort, boolean shouldReportMetrics) throws SocketException, InterruptedException
  {
    CacheConfig.setOnMaster(conf, false);
    if (shouldReportMetrics) {
      CacheConfig.setMetricsReporters(conf, MetricsReporter.STATSD.name());
    }

    return startServersForTestingStatsDReporter(statsDPort, testCasePort);
  }

  /**
   * Start & configure the servers necessary for running & testing StatsDReporter.
   *
   * @param statsDPort The port to send StatsD metrics to.
   * @param testCasePort The port from which to receive responses from the StatsD
   * @throws SocketException if the socket for the mock StatsD server could not be created or bound.
   * @throws InterruptedException if the current thread is interrupted while sleeping.
   */
  private MockStatsDThread startServersForTestingStatsDReporter(int statsDPort, int testCasePort) throws SocketException, InterruptedException
  {
    CacheConfig.setStatsDMetricsPort(conf, statsDPort);
    CacheConfig.setMetricsReportingInterval(conf, 1000);

    MockStatsDThread statsDThread = new MockStatsDThread(statsDPort, testCasePort);
    statsDThread.start();

    startMockCoordinatorBookKeeperServer(conf, metrics);
    return statsDThread;
  }

  /**
   * Check if StatsDReporter is correctly firing metrics.
   *
   * @param receivePort The port on which to receive responses from the mock StatsD server.
   * @return True if the mock StatsDReporter has received a response
   * @throws IOException if an I/O error occurs while waiting for a response.
   */
  private boolean isStatsDReporterFiring(int receivePort) throws IOException
  {
    byte[] data = new byte[TEST_PACKET_SIZE];
    DatagramSocket socket = new DatagramSocket(receivePort);
    DatagramPacket packet = new DatagramPacket(data, data.length);

    socket.setSoTimeout(TEST_SOCKET_TIMEOUT);
    try {
      socket.receive(packet);
    }
    catch (SocketTimeoutException e) {
      return false;
    }

    return true;
  }
//
//  /**
//   * Class to mock the behaviour of {@link BookKeeperServer} for testing registering & reporting metrics.
//   */
//  private static class MockBookKeeperServer extends BookKeeperServer
//  {
//    public void startServer(Configuration conf, MetricRegistry metricRegistry)
//    {
//      try {
//        new CoordinatorBookKeeper(conf, metricRegistry);
//      }
//      catch (FileNotFoundException e) {
//        log.error("Cache directories could not be created", e);
//        return;
//      }
//
//      metrics = metricRegistry;
//      registerMetrics(conf);
//    }
//
//    public void stopServer()
//    {
//      removeMetrics();
//    }
//  }

  /**
   * Thread to capture UDP requests from StatsDReporter intended for StatsD.
   */
  private static class MockStatsDThread extends Thread
  {
    private volatile boolean isRunning = true;

    // The socket to send/receive StatsD metrics from.
    private final DatagramSocket socket;

    // The port the current test case is expecting to receive acknowledgement from.
    private final int testCasePort;

    private MockStatsDThread(int statsDPort, int testCasePort) throws SocketException
    {
      this.socket = new DatagramSocket(statsDPort);
      this.testCasePort = testCasePort;
    }

    @Override
    public void run()
    {
      while (isRunning) {
        try {
          byte[] response = new byte[TEST_PACKET_SIZE];
          final DatagramPacket receivedPacket = new DatagramPacket(response, response.length);
          socket.receive(receivedPacket);

          String responseMessage = "Received from MockStatsD";
          response = responseMessage.getBytes();
          socket.send(new DatagramPacket(response, response.length, receivedPacket.getAddress(), testCasePort));
        }
        catch (IOException e) {
          log.error("Error sending/receiving UDP packets", e);
        }
      }
    }

    public void stopThread()
    {
      isRunning = false;
    }
  }
}
