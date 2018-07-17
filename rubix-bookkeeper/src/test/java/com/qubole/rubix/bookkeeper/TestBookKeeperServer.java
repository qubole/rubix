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
import com.qubole.rubix.bookkeeper.metrics.MetricsReporter;
import com.qubole.rubix.bookkeeper.test.BookKeeperTest;
import com.qubole.rubix.core.utils.DeleteFileVisitor;
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
import javax.management.ObjectName;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Set;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestBookKeeperServer extends BookKeeperTest
{
  private static final Log log = LogFactory.getLog(TestBookKeeperServer.class.getName());
  private static final String cacheTestDirPrefix = System.getProperty("java.io.tmpdir") + "/bookKeeperServerTest/";
  private static final int TEST_MAX_DISKS = 1;
  private static final int PACKET_SIZE = 32;
  private static final int SOCKET_TIMEOUT = 5000;
  private static final String JMX_METRIC_NAME_PATTERN = "metrics:*";

  private final MetricRegistry metrics = new MetricRegistry();
  private final Configuration conf = new Configuration();

  @BeforeClass
  public void initializeCacheDirectories() throws IOException
  {
    // Create cache directories
    Files.createDirectories(Paths.get(cacheTestDirPrefix));
    for (int i = 0; i < TEST_MAX_DISKS; i++) {
      Files.createDirectories(Paths.get(cacheTestDirPrefix + i));
    }
  }

  @BeforeMethod
  public void setUp()
  {
    CacheConfig.setCacheDataDirPrefix(conf, cacheTestDirPrefix);
    CacheConfig.setMaxDisks(conf, TEST_MAX_DISKS);
  }

  @AfterMethod
  public void stopBookKeeperServerForTest()
  {
    conf.clear();
    metrics.removeMatching(MetricFilter.ALL);

    stopMockBookKeeperServer();
  }

  @AfterClass
  public void cleanUpCacheDirectories() throws IOException
  {
    Files.walkFileTree(Paths.get(cacheTestDirPrefix), new DeleteFileVisitor());
    Files.deleteIfExists(Paths.get(cacheTestDirPrefix));
  }

  /**
   * Verify that liveness metrics are registered when configured to.
   */
  @Test
  public void testLivenessMetricsEnabled() throws InterruptedException
  {
    super.testLivenessMetrics(ServerType.BOOKKEEPER, conf, metrics, true);
  }

  /**
   * Verify that liveness metrics are not registered when configured not to.
   */
  @Test
  public void testLivenessMetricsNotEnabled() throws InterruptedException
  {
    super.testLivenessMetrics(ServerType.BOOKKEEPER, conf, metrics, false);
  }

  /**
   * Verify that cache metrics are registered when configured to.
   */
  @Test
  public void testCacheMetricsEnabled() throws InterruptedException
  {
    super.testCacheMetrics(ServerType.BOOKKEEPER, conf, metrics, true);
  }

  /**
   * Verify that cache metrics are not registered when configured not to.
   */
  @Test
  public void testCacheMetricsNotEnabled() throws InterruptedException
  {
    super.testCacheMetrics(ServerType.BOOKKEEPER, conf, metrics, false);
  }

  /**
   * Verify that JVM metrics are registered when configured to.
   */
  @Test
  public void testJvmMetricsEnabled() throws InterruptedException
  {
    super.testJvmMetrics(ServerType.BOOKKEEPER, conf, metrics, true);
  }

  /**
   * Verify that JVM metrics are not registered when configured not to.
   */
  @Test
  public void testJvmMetricsNotEnabled() throws InterruptedException
  {
    super.testJvmMetrics(ServerType.BOOKKEEPER, conf, metrics, false);
  }

  /**
   * Verify that all registered metrics are removed once the BookKeeper server has stopped.
   */
  @Test
  public void verifyMetricsAreRemoved() throws InterruptedException
  {
    super.verifyMetricsAreRemoved(ServerType.BOOKKEEPER, conf, metrics);
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

    startServersForTestingStatsDReporterOnMaster(statsDPort, testCasePort, shouldReport);

    assertTrue(isStatsDReporterFiring(testCasePort), "BookKeeperServer is not reporting to StatsD");
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

    startServersForTestingStatsDReporterOnMaster(statsDPort, testCasePort, shouldReport);

    assertFalse(isStatsDReporterFiring(testCasePort), "BookKeeperServer should not report to StatsD");
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

    startServersForTestingStatsDReporterForWorker(statsDPort, testCasePort, shouldReport);

    assertTrue(isStatsDReporterFiring(testCasePort), "BookKeeperServer is not reporting to StatsD");
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

    startServersForTestingStatsDReporterForWorker(statsDPort, testCasePort, shouldReport);

    assertFalse(isStatsDReporterFiring(testCasePort), "BookKeeperServer should not report to StatsD");
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

    Set<ObjectName> metricsObjectNames = getJmxObjectNamesWithPattern(JMX_METRIC_NAME_PATTERN);
    for (ObjectName name : metricsObjectNames) {
      System.out.println("YES: " + name.toString());
    }
    assertTrue(metricsObjectNames.size() == 0, "Metrics should not be registered with JMX before server starts.");

    startMockBookKeeperServer(conf, metrics);

    metricsObjectNames = getJmxObjectNamesWithPattern(JMX_METRIC_NAME_PATTERN);
    assertTrue(metricsObjectNames.size() > 0, "Metrics should be registered with JMX after server starts.");

    stopMockBookKeeperServer();

    metricsObjectNames = getJmxObjectNamesWithPattern(JMX_METRIC_NAME_PATTERN);
    assertTrue(metricsObjectNames.size() == 0, "Metrics should not be registered with JMX after server stops.");
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

    startMockBookKeeperServer(conf, metrics);

    final Set<ObjectName> metricsObjectNames = getJmxObjectNamesWithPattern(JMX_METRIC_NAME_PATTERN);
    assertTrue(metricsObjectNames.size() == 0, "Metrics should not be registered with JMX.");

    stopMockBookKeeperServer();
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
  private void startServersForTestingStatsDReporterOnMaster(int statsDPort, int testCasePort, boolean shouldReportMetrics) throws SocketException, InterruptedException
  {
    CacheConfig.setOnMaster(conf, true);
    if (shouldReportMetrics) {
      CacheConfig.setMetricsReporters(conf, MetricsReporter.STATSD.name());
    }

    startServersForTestingStatsDReporter(statsDPort, testCasePort);
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
  private void startServersForTestingStatsDReporterForWorker(int statsDPort, int testCasePort, boolean shouldReportMetrics) throws SocketException, InterruptedException
  {
    CacheConfig.setOnMaster(conf, false);
    if (shouldReportMetrics) {
      CacheConfig.setMetricsReporters(conf, MetricsReporter.STATSD.name());
    }

    startServersForTestingStatsDReporter(statsDPort, testCasePort);
  }

  /**
   * Start & configure the servers necessary for running & testing StatsDReporter.
   *
   * @param statsDPort The port to send StatsD metrics to.
   * @param testCasePort The port from which to receive responses from the StatsD
   * @throws SocketException if the socket for the mock StatsD server could not be created or bound.
   * @throws InterruptedException if the current thread is interrupted while sleeping.
   */
  private void startServersForTestingStatsDReporter(int statsDPort, int testCasePort) throws SocketException, InterruptedException
  {
    CacheConfig.setStatsDMetricsPort(conf, statsDPort);
    CacheConfig.setStatsDMetricsInterval(conf, 1000);

    new MockStatsDThread(statsDPort, testCasePort).start();
    startMockBookKeeperServer(conf, metrics);
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
    byte[] data = new byte[PACKET_SIZE];
    DatagramSocket socket = new DatagramSocket(receivePort);
    DatagramPacket packet = new DatagramPacket(data, data.length);

    socket.setSoTimeout(SOCKET_TIMEOUT);
    try {
      socket.receive(packet);
    }
    catch (SocketTimeoutException e) {
      return false;
    }

    return true;
  }

  /**
   * Get the set of JMX object names matching the provided pattern.
   *
   * @param pattern The object name pattern to match.
   * @return The set of ObjectNames that match the given pattern.
   * @throws MalformedObjectNameException if the format of the pattern string does not correspond to a valid ObjectName.
   */
  private Set<ObjectName> getJmxObjectNamesWithPattern(String pattern) throws MalformedObjectNameException
  {
    return ManagementFactory.getPlatformMBeanServer().queryNames(new ObjectName(pattern), null);
  }

  /**
   * Thread to capture UDP requests from StatsDReporter intended for StatsD.
   */
  private static class MockStatsDThread extends Thread
  {
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
      while (true) {
        try {
          byte[] response = new byte[PACKET_SIZE];
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
  }
}
