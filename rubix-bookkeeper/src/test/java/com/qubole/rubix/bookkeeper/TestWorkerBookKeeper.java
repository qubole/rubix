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
import com.google.common.base.Ticker;
import com.google.common.testing.FakeTicker;
import com.qubole.rubix.bookkeeper.exception.BookKeeperInitializationException;
import com.qubole.rubix.common.metrics.BookKeeperMetrics;
import com.qubole.rubix.common.utils.TestUtil;
import com.qubole.rubix.spi.BookKeeperFactory;
import com.qubole.rubix.spi.CacheConfig;
import com.qubole.rubix.spi.RetryingBookkeeperClient;
import com.qubole.rubix.spi.thrift.ClusterNode;
import com.qubole.rubix.spi.thrift.HeartbeatStatus;
import com.qubole.rubix.spi.thrift.NodeState;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.shaded.transport.TSocket;
import org.apache.thrift.shaded.transport.TTransportException;
import org.mockito.ArgumentMatchers;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestWorkerBookKeeper
{
  private static final Log log = LogFactory.getLog(TestWorkerBookKeeper.class);

  private static final String TEST_CACHE_DIR_PREFIX = TestUtil.getTestCacheDirPrefix("TestWorkerBookKeeper");
  private static final int TEST_MAX_DISKS = 1;

  private final Configuration conf = new Configuration();

  @BeforeClass
  public void setUpForClass() throws IOException
  {
    CacheConfig.setCacheDataDirPrefix(conf, TEST_CACHE_DIR_PREFIX);

    TestUtil.createCacheParentDirectories(conf, TEST_MAX_DISKS);
  }

  @BeforeMethod
  public void setUp() throws InterruptedException
  {
    CacheConfig.setCacheDataDirPrefix(conf, TEST_CACHE_DIR_PREFIX);
    CacheConfig.setMaxDisks(conf, TEST_MAX_DISKS);
    CacheConfig.setOnMaster(conf, true);
    CacheConfig.setMetricsReporters(conf, "");
    BaseServerTest.startCoordinatorBookKeeperServer(conf, new MetricRegistry());
  }

  @AfterMethod
  public void tearDown()
  {
    conf.clear();

    BaseServerTest.stopBookKeeperServer();
  }

  @AfterClass
  public void tearDownForClass() throws IOException
  {
    CacheConfig.setCacheDataDirPrefix(conf, TEST_CACHE_DIR_PREFIX);

    TestUtil.removeCacheParentDirectories(conf, TEST_MAX_DISKS);
  }

  /**
   * Verify that WorkerBookKeeper throws the correct exception when asked to handle heartbeats.
   *
   * @throws BookKeeperInitializationException if the parent directory for the cache cannot be found when initializing the BookKeeper.
   */
  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void testHandleHeartbeat_shouldNotBeHandled() throws BookKeeperInitializationException, IOException
  {
    // Disable default reporters for this BookKeeper, since they will conflict with the running server.
    CacheConfig.setMetricsReporters(conf, "");
    try (BookKeeperMetrics bookKeeperMetrics = new BookKeeperMetrics(conf, new MetricRegistry())) {
      final WorkerBookKeeper workerBookKeeper = new WorkerBookKeeper(conf, bookKeeperMetrics);
      workerBookKeeper.handleHeartbeat("", new HeartbeatStatus());
    }
  }

  @Test
  public void testGetOwnerNodeForPathFromCoordinatorAndThenWorkerCache() throws TTransportException,
      BookKeeperInitializationException, InterruptedException, IOException
  {
    try (final BookKeeperMetrics bookKeeperMetrics = new BookKeeperMetrics(conf, new MetricRegistry())) {
      final CoordinatorBookKeeper spyCoordinator = spy(new CoordinatorBookKeeper(conf, bookKeeperMetrics));
      final BookKeeperServer bookKeeperServer = new BookKeeperServer();

      CacheConfig.setServerPort(conf, 1234);
      final Thread thread = new Thread() {
        public void run()
        {
          bookKeeperServer.startServer(conf, spyCoordinator, bookKeeperMetrics);
        }
      };
      thread.start();

      while (!bookKeeperServer.isServerUp()) {
        Thread.sleep(200);
        log.info("Waiting for BookKeeper Server to come up");
      }

      String testLocalhost = "localhost_test";
      String changedTestLocalhost = "changed_localhost";
      List<ClusterNode> nodes = new ArrayList<>();

      nodes.add(new ClusterNode(testLocalhost, NodeState.ACTIVE));

      doReturn(nodes).when(spyCoordinator).getClusterNodes();

      BookKeeperFactory factory = new BookKeeperFactory();
      final BookKeeperFactory bookKeeperFactory = spy(factory);

      TSocket socket = new TSocket("localhost", 1234, CacheConfig.getServerConnectTimeout(conf));
      socket.open();

      doReturn(new RetryingBookkeeperClient(socket, CacheConfig.getMaxRetries(conf)))
          .when(bookKeeperFactory).createBookKeeperClient(anyString(), ArgumentMatchers.<Configuration>any());

      FakeTicker ticker = new FakeTicker();

      CacheConfig.setWorkerNodeInfoExpiryPeriod(conf, 100);
      try (final BookKeeperMetrics workerMetrics = new BookKeeperMetrics(conf, new MetricRegistry())) {
        final WorkerBookKeeper workerBookKeeper = new WorkerBookKeeper(conf, workerMetrics, ticker, bookKeeperFactory);
        String hostName = workerBookKeeper.getOwnerNodeForPath("remotepath");

        assertTrue(hostName.equals(testLocalhost), "HostName is not correct from the coordinator");

        nodes.clear();
        nodes.add(new ClusterNode(changedTestLocalhost, NodeState.ACTIVE));

        doReturn(nodes).when(spyCoordinator).getClusterNodes();
        hostName = workerBookKeeper.getOwnerNodeForPath("remotepath");

        assertTrue(hostName.equals(testLocalhost), "HostName is not correct from the cache");
        ticker.advance(500, TimeUnit.SECONDS);

        hostName = workerBookKeeper.getOwnerNodeForPath("remotepath");
        assertTrue(hostName.equals(changedTestLocalhost), "HostName is not refreshed from Coordinator");
      }

      bookKeeperServer.stopServer();
    }
  }

  @Test
  public void testGetClusterNodeHostNameWhenCoordinatorIsDown() throws TTransportException, BookKeeperInitializationException, IOException
  {
    CacheConfig.setServiceMaxRetries(conf, 1);
    CacheConfig.setServiceRetryInterval(conf, 1);
    BookKeeperFactory bookKeeperFactory = new BookKeeperFactory();
    final BookKeeperFactory spyBookKeeperFactory = spy(bookKeeperFactory);
    doThrow(TTransportException.class).when(spyBookKeeperFactory).createBookKeeperClient(anyString(), ArgumentMatchers.<Configuration>any());
    FakeTicker ticker = new FakeTicker();
    String hostName = "";
    try (BookKeeperMetrics bookKeeperMetrics = new BookKeeperMetrics(conf, new MetricRegistry())) {
      final WorkerBookKeeper workerBookKeeper = new MockWorkerBookKeeper(conf, bookKeeperMetrics, ticker, spyBookKeeperFactory);
      hostName = workerBookKeeper.getOwnerNodeForPath("remotePath");
    }
    assertNull(hostName, "HostName should be null as Cooordinator is down");
  }

  private class MockWorkerBookKeeper extends WorkerBookKeeper
  {
    public MockWorkerBookKeeper(Configuration conf, BookKeeperMetrics bookKeeperMetrics, Ticker ticker, BookKeeperFactory factory) throws BookKeeperInitializationException
    {
      super(conf, bookKeeperMetrics, ticker, factory);
    }

    @Override
    void startHeartbeatService(Configuration conf, MetricRegistry metrics, BookKeeperFactory factory)
    {
      return;
    }

    @Override
    void setCurrentNodeName()
    {
      nodeName = "localhost";
    }
  }
}
