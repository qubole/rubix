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
import com.google.common.base.Ticker;
import com.google.common.testing.FakeTicker;
import com.qubole.rubix.common.utils.TestUtil;
import com.qubole.rubix.spi.BookKeeperFactory;
import com.qubole.rubix.spi.CacheConfig;
import com.qubole.rubix.spi.ClusterType;
import com.qubole.rubix.spi.RetryingBookkeeperClient;
import com.qubole.rubix.spi.thrift.HeartbeatStatus;
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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.anyInt;
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
   * @throws FileNotFoundException if the parent directory for the cache cannot be found when initializing the BookKeeper.
   */
  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void testHandleHeartbeat_shouldNotBeHandled() throws FileNotFoundException
  {
    final WorkerBookKeeper workerBookKeeper = new WorkerBookKeeper(conf, new MetricRegistry());
    workerBookKeeper.handleHeartbeat("", new HeartbeatStatus());
  }

  @Test
  public void testGetClusterNodeHostNameFromCoordinatorAndThenWorkerCache() throws FileNotFoundException, TTransportException, InterruptedException
  {
    List<String> mockList = new ArrayList<>();

    final MetricRegistry metricRegistry = new MetricRegistry();
    final CoordinatorBookKeeper spyCoordinator = spy(new CoordinatorBookKeeper(conf, metricRegistry));
    final BookKeeperServer bookKeeperServer = new BookKeeperServer();

    CacheConfig.setServerPort(conf, 1234);
    final Thread thread = new Thread()
    {
      public void run()
      {
        bookKeeperServer.startServer(conf, new MetricRegistry(), spyCoordinator);
      }
    };
    thread.start();

    while (!bookKeeperServer.isServerUp()) {
      Thread.sleep(200);
      log.info("Waiting for BookKeeper Server to come up");
    }

    String testLocalhost = "localhost_test";
    String changedTestLocalhost = "changed_localhost";
    mockList.add(testLocalhost);

    doReturn(mockList).when(spyCoordinator).getNodeHostNames(anyInt());

    BookKeeperFactory factory = new BookKeeperFactory();
    final BookKeeperFactory bookKeeperFactory = spy(factory);

    TSocket socket = new TSocket("localhost", 1234, CacheConfig.getClientTimeout(conf));
    socket.open();

    doReturn(new RetryingBookkeeperClient(socket, CacheConfig.getMaxRetries(conf)))
        .when(bookKeeperFactory).createBookKeeperClient(anyString(), ArgumentMatchers.<Configuration>any());

    FakeTicker ticker = new FakeTicker();

    CacheConfig.setWorkerNodeInfoExpiryPeriod(conf, 100);
    final WorkerBookKeeper workerBookKeeper = new WorkerBookKeeper(conf, new MetricRegistry(), ticker, bookKeeperFactory);
    String hostName = workerBookKeeper.getClusterNodeHostName("remotepath", ClusterType.TEST_CLUSTER_MANAGER.ordinal());

    assertTrue(hostName.equals(testLocalhost), "HostName is not correct from the coordinator");

    mockList.clear();
    mockList.add(changedTestLocalhost);

    doReturn(mockList).when(spyCoordinator).getNodeHostNames(anyInt());
    hostName = workerBookKeeper.getClusterNodeHostName("remotepath", ClusterType.TEST_CLUSTER_MANAGER.ordinal());

    assertTrue(hostName.equals(testLocalhost), "HostName is not correct from the cache");
    ticker.advance(500, TimeUnit.SECONDS);

    // This call is to trigger refreshing of NodesCache list in the workerbookkeeper.
    // Until the value is refreshed, the cache is going to return the older value.
    // Adding a thread.sleep to make sure the loading is complete. The next call is going to fetch the actual data.

    hostName = workerBookKeeper.getClusterNodeHostName("remotepath", ClusterType.TEST_CLUSTER_MANAGER.ordinal());
    Thread.sleep(1000);
    hostName = workerBookKeeper.getClusterNodeHostName("remotepath", ClusterType.TEST_CLUSTER_MANAGER.ordinal());

    assertTrue(hostName.equals(changedTestLocalhost), "HostName is not refreshed from Coordinator");

    bookKeeperServer.stopServer();
  }

  @Test
  public void testGetClusterNodeHostNameWhenCoordinatorIsDown() throws FileNotFoundException, TTransportException
  {
    CacheConfig.setServiceMaxRetries(conf, 1);
    CacheConfig.setServiceRetryInterval(conf, 1);
    BookKeeperFactory bookKeeperFactory = new BookKeeperFactory();
    final BookKeeperFactory spyBookKeeperFactory = spy(bookKeeperFactory);
    doThrow(TTransportException.class).when(spyBookKeeperFactory).createBookKeeperClient(anyString(), ArgumentMatchers.<Configuration>any());
    FakeTicker ticker = new FakeTicker();
    final WorkerBookKeeper workerBookKeeper = new MockWorkerBookKeeper(conf, new MetricRegistry(), ticker, spyBookKeeperFactory);
    String hostName = workerBookKeeper.getClusterNodeHostName("remotePath", ClusterType.TEST_CLUSTER_MANAGER.ordinal());
    assertNull(hostName, "HostName should be null as Cooordinator is down");
  }

  private class MockWorkerBookKeeper extends WorkerBookKeeper
  {
    public MockWorkerBookKeeper(Configuration conf, MetricRegistry metrics, Ticker ticker, BookKeeperFactory factory) throws FileNotFoundException
    {
      super(conf, metrics, ticker, factory);
    }
    @Override
    void startHeartbeatService(Configuration conf, MetricRegistry metrics, BookKeeperFactory factory)
    {
      return;
    }
  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void testWorkerGetHostNames() throws FileNotFoundException
  {
    final WorkerBookKeeper workerBookKeeper = new WorkerBookKeeper(conf, new MetricRegistry());
    workerBookKeeper.getNodeHostNames(ClusterType.TEST_CLUSTER_MANAGER.ordinal());
  }
}
