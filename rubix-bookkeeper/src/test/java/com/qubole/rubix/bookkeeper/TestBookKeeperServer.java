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
import com.qubole.rubix.bookkeeper.test.BookKeeperTestUtils;
import com.qubole.rubix.spi.CacheConfig;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.net.SocketTimeoutException;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestBookKeeperServer
{
  private static final Log log = LogFactory.getLog(TestBookKeeperServer.class);

  private static final String TEST_CACHE_DIR_PREFIX = BookKeeperTestUtils.getTestCacheDirPrefix("TestBookKeeperServer");
  private static final int TEST_MAX_DISKS = 1;
  private static final int TEST_PACKET_SIZE = 32;
  private static final int TEST_SOCKET_TIMEOUT = 5000;

  private final Configuration conf = new Configuration();
  private final MetricRegistry metrics = new MetricRegistry();

  @BeforeClass
  public void setUpForClass() throws IOException
  {
    CacheConfig.setCacheDataDirPrefix(conf, TEST_CACHE_DIR_PREFIX);

    BookKeeperTestUtils.createCacheParentDirectories(conf, TEST_MAX_DISKS);
  }

  @BeforeMethod
  public void setUp()
  {
    CacheConfig.setCacheDataDirPrefix(conf, TEST_CACHE_DIR_PREFIX);
  }

  @AfterMethod
  public void tearDown()
  {
    conf.clear();
    metrics.removeMatching(MetricFilter.ALL);

    stopBookKeeperServer();
  }

  @AfterClass
  public void tearDownForClass() throws IOException
  {
    CacheConfig.setCacheDataDirPrefix(conf, TEST_CACHE_DIR_PREFIX);

    BookKeeperTestUtils.removeCacheParentDirectories(conf, TEST_MAX_DISKS);
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

    MockStatsDThread statsDThread = startServersForTestingStatsDReporterOnMaster(statsDPort, testCasePort, shouldReport);

    try {
      assertTrue(isStatsDReporterFiring(testCasePort), "BookKeeperServer is not reporting to StatsD");
    }
    finally {
      statsDThread.stopThread();
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
    }
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
  private MockStatsDThread startServersForTestingStatsDReporterOnMaster(int statsDPort, int testCasePort, boolean shouldReportMetrics) throws SocketException, InterruptedException
  {
    CacheConfig.setOnMaster(conf, true);
    CacheConfig.setReportStatsdMetricsOnMaster(conf, shouldReportMetrics);

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
    CacheConfig.setReportStatsdMetricsOnWorker(conf, shouldReportMetrics);

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
    CacheConfig.setStatsDMetricsInterval(conf, 1000);

    MockStatsDThread statsDThread = new MockStatsDThread(statsDPort, testCasePort);
    statsDThread.start();

    startBookKeeperServer();
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

  /**
   * Class to mock the behaviour of {@link BookKeeperServer} for testing registering & reporting metrics.
   */
  private static class MockBookKeeperServer extends BookKeeperServer
  {
    public static void startServer(Configuration conf, MetricRegistry metricRegistry)
    {
      try {
        new CoordinatorBookKeeper(conf, metricRegistry);
      }
      catch (FileNotFoundException e) {
        log.error("Cache directories could not be created", e);
        return;
      }

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
