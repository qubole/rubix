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
import com.qubole.rubix.common.TestUtil;
import com.qubole.rubix.spi.BookKeeperFactory;
import com.qubole.rubix.spi.CacheConfig;
import com.qubole.rubix.spi.ClusterType;
import com.qubole.rubix.spi.RetryingBookkeeperClient;
import com.qubole.rubix.spi.thrift.CacheStatusRequest;
import com.qubole.rubix.spi.thrift.HeartbeatStatus;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.shaded.transport.TSocket;
import org.mockito.ArgumentMatchers;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.FileNotFoundException;
import java.io.IOException;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
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
    CacheConfig.setBlockSize(conf, 200);

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
  public void veryifyProperInitializationOfClusterManagers() throws Exception
  {
    final BookKeeperFactory bookKeeperFactory = mock(BookKeeperFactory.class);
    when(bookKeeperFactory.createBookKeeperClient(anyString(), ArgumentMatchers.<Configuration>any())).thenReturn(
        new RetryingBookkeeperClient(
            new TSocket("localhost", CacheConfig.getServerPort(conf), CacheConfig.getClientTimeout(conf)),
            CacheConfig.getMaxRetries(conf)));

    MetricRegistry registry = new MetricRegistry();
    String filePath = "File1";
    WorkerBookKeeper worker = new WorkerBookKeeper(conf, registry, bookKeeperFactory);
    CacheStatusRequest request = new CacheStatusRequest(filePath, 1000, 1000, 0, 1, ClusterType.TEST_CLUSTER_MANAGER_MULTINODE.ordinal());
    worker.getCacheStatus(request);

    assertTrue(worker.getClusterManagerMap().size() == 2, "There should be two instances of ClusterManagers");
    assertTrue(worker.getClusterManagerMap().containsKey(ClusterType.TEST_CLUSTER_MANAGER.ordinal()),
        "There should be a clustermanager instance for TestClusterManager");
    assertTrue(worker.getClusterManagerMap().containsKey(ClusterType.TEST_CLUSTER_MANAGER_MULTINODE.ordinal()),
        "There should be a clustermanager instance for TestClusterManager Multinode");
  }
}
