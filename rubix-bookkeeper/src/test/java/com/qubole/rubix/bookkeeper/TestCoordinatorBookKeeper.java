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

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.testing.FakeTicker;
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
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertEquals;

public class TestCoordinatorBookKeeper
{
  private static final Log log = LogFactory.getLog(TestCoordinatorBookKeeper.class);

  private static final String TEST_CACHE_DIR_PREFIX = BookKeeperTestUtils.getTestCacheDirPrefix("TestCoordinatorBookKeeper");
  private static final String TEST_HOSTNAME_WORKER1 = "worker1";
  private static final String TEST_HOSTNAME_WORKER2 = "worker2";
  private static final int TEST_MAX_DISKS = 1;

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
  }

  @AfterClass
  public void tearDownForClass() throws IOException
  {
    CacheConfig.setCacheDataDirPrefix(conf, TEST_CACHE_DIR_PREFIX);

    BookKeeperTestUtils.removeCacheParentDirectories(conf, TEST_MAX_DISKS);
  }

  /**
   * Verify that the worker liveness count metric is correctly registered.
   */
  @Test
  public void testWorkerLivenessCountMetric() throws FileNotFoundException
  {
    final CoordinatorBookKeeper coordinatorBookKeeper = new CoordinatorBookKeeper(conf, metrics);
    coordinatorBookKeeper.handleHeartbeat(TEST_HOSTNAME_WORKER1);
    coordinatorBookKeeper.handleHeartbeat(TEST_HOSTNAME_WORKER2);

    int workerCount = (int) metrics.getGauges().get(CoordinatorBookKeeper.METRIC_BOOKKEEPER_LIVE_WORKER_GAUGE).getValue();
    assertEquals(workerCount, 2, "Incorrect number of workers reporting heartbeat");
  }

  /**
   * Verify that the worker liveness status properly expires.
   */
  @Test
  public void testWorkerLivenessCountMetric_workerLivenessExpired() throws FileNotFoundException
  {
    final FakeTicker ticker = new FakeTicker();
    final int workerLivenessExpiry = 1000; // ms
    CacheConfig.setWorkerLivenessExpiry(conf, workerLivenessExpiry);

    final CoordinatorBookKeeper coordinatorBookKeeper = new CoordinatorBookKeeper(conf, metrics, ticker);
    final Gauge liveWorkerGauge = metrics.getGauges().get(CoordinatorBookKeeper.METRIC_BOOKKEEPER_LIVE_WORKER_GAUGE);

    coordinatorBookKeeper.handleHeartbeat(TEST_HOSTNAME_WORKER1);
    coordinatorBookKeeper.handleHeartbeat(TEST_HOSTNAME_WORKER2);

    int workerCount = (int) liveWorkerGauge.getValue();
    assertEquals(workerCount, 2, "Incorrect number of workers reporting heartbeat");

    ticker.advance(workerLivenessExpiry, TimeUnit.MILLISECONDS);
    coordinatorBookKeeper.handleHeartbeat(TEST_HOSTNAME_WORKER1);

    workerCount = (int) liveWorkerGauge.getValue();
    assertEquals(workerCount, 1, "Incorrect number of workers reporting heartbeat");
  }
}
