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

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestBookKeeperServer
{
  private static final String cacheTestDirPrefix = System.getProperty("java.io.tmpdir") + "/bookKeeperServerTest/";
  private static final Log log = LogFactory.getLog(TestBookKeeperServer.class.getName());
  private static final int PACKET_SIZE = 32;
  private static final int SOCKET_TIMEOUT = 5000;

  private MetricRegistry metrics;
  private Configuration conf = new Configuration();

  @BeforeClass
  public void initializeCacheDirectories() throws IOException
  {
    // Set configuration values for testing
    CacheConfig.setCacheDataDirPrefix(conf, cacheTestDirPrefix);
    CacheConfig.setMaxDisks(conf, 5);

    // Create cache directories
    Files.createDirectories(Paths.get(cacheTestDirPrefix));
    for (int i = 0; i < CacheConfig.getCacheMaxDisks(conf); i++) {
      Files.createDirectories(Paths.get(cacheTestDirPrefix, String.valueOf(i)));
    }
  }

  @BeforeMethod
  public void setUp()
  {
    metrics = new MetricRegistry();
  }

  @AfterMethod
  public void stopBookKeeperServerForTest()
  {
    stopBookKeeperServer();
  }

  @AfterClass
  public void cleanUpCacheDirectories() throws IOException
  {
    Files.walkFileTree(Paths.get(cacheTestDirPrefix), new DeleteFileVisitor());
    Files.deleteIfExists(Paths.get(cacheTestDirPrefix));
  }

  /**
   * Verify that liveness status of the BookKeeper daemon is correctly reported.
   */
  @Test
  public void verifyLivenessCheck()
  {
    CacheConfig.setOnMaster(conf, true);

    assertNull(metrics.getGauges().get(BookKeeperServer.METRIC_BOOKKEEPER_LIVENESS_CHECK), "Metric should not exist before server has started");

    startBookKeeperServer();

    assertEquals(metrics.getGauges().get(BookKeeperServer.METRIC_BOOKKEEPER_LIVENESS_CHECK).getValue(), 1, "Metric should return a value once the server has started");

    stopBookKeeperServer();

    assertNull(metrics.getGauges().get(BookKeeperServer.METRIC_BOOKKEEPER_LIVENESS_CHECK), "Metric should not exist after server has stopped");
  }

  /**
   * Start an instance of the BookKeeper server.
   */
  private void startBookKeeperServer()
  {
    MockBookKeeperServer.startServer(conf, metrics);
  }

  /**
   * Stop the currently running BookKeeper server instance.
   */
  private void stopBookKeeperServer()
  {
    MockBookKeeperServer.stopServer();
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
   * Verify that all registered metrics are removed once the BookKeeper server has stopped.
   */
  @Test
  public void verifyMetricsAreRemoved()
  {
    assertTrue(metrics.getNames().size() == 0, "Metrics should not be registered before server is started.");

    startBookKeeperServer();

    assertTrue(metrics.getNames().size() > 0, "Metrics should be registered once server is started.");

    stopBookKeeperServer();

    assertTrue(metrics.getNames().size() == 0, "Metrics should not be registered after server has stopped.");
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
    CacheConfig.setReportStatsdMetricsOnMaster(conf, shouldReportMetrics);

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
    CacheConfig.setReportStatsdMetricsOnWorker(conf, shouldReportMetrics);

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
    startBookKeeperServer();
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
   * Class to mock the behaviour of {@link BookKeeperServer} for testing registering & reporting metrics.
   */
  private static class MockBookKeeperServer extends BookKeeperServer
  {
    public static void startServer(Configuration conf, MetricRegistry metricRegistry)
    {
      metrics = metricRegistry;
      registerMetrics(conf);
    }

    public static void stopServer()
    {
      removeMetrics();
    }
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
