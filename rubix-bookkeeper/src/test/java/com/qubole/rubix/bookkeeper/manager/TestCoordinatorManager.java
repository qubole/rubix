/**
 * Copyright (c) 2016. Qubole Inc
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

package com.qubole.rubix.bookkeeper.manager;

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Ticker;
import com.google.common.cache.CacheBuilder;
import com.google.common.testing.FakeTicker;
import com.qubole.rubix.bookkeeper.CoordinatorBookKeeper;
import com.qubole.rubix.spi.CacheConfig;
import com.qubole.rubix.spi.CacheUtil;
import org.apache.hadoop.conf.Configuration;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class TestCoordinatorManager
{
  private static final String cacheTestDirPrefix = System.getProperty("java.io.tmpdir") + "/coordinatorManagerTest/";
  private static final int maxDisks = 5;
  private static final String WORKER1_HOSTNAME = "worker1";
  private static final String WORKER2_HOSTNAME = "worker2";

  private Configuration conf;
  private MetricRegistry metrics;

  @BeforeClass
  public void initializeCacheDirectories() throws IOException
  {
    this.conf = new Configuration();
    Files.createDirectories(Paths.get(cacheTestDirPrefix));
    for (int i = 0; i < maxDisks; i++) {
      Files.createDirectories(Paths.get(cacheTestDirPrefix, String.valueOf(i)));
    }

    CacheConfig.setCacheDataDirPrefix(conf, cacheTestDirPrefix);
    createCacheDirectoriesForTest(conf);
  }

  @BeforeMethod
  public void setUp()
  {
    conf.clear();
    this.metrics = new MetricRegistry();
  }

  /**
   * Verify that the worker liveness count metric is correctly registered.
   */
  @Test
  public void testWorkerLivenessCountMetric() throws FileNotFoundException
  {
    CacheConfig.setCacheDataDirPrefix(conf, cacheTestDirPrefix);

    final CoordinatorBookKeeper coordinatorBookKeeper = new CoordinatorBookKeeper(conf, metrics);
    coordinatorBookKeeper.handleHeartbeat(WORKER1_HOSTNAME);
    coordinatorBookKeeper.handleHeartbeat(WORKER2_HOSTNAME);

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
    final int workerLivenessExpiry = 5000; // ms
    CacheConfig.setWorkerLivenessExpiry(conf, workerLivenessExpiry);
    CacheConfig.setCacheDataDirPrefix(conf, cacheTestDirPrefix);

    final MockCoordinatorBookKeeper coordinatorManager = new MockCoordinatorBookKeeper(conf, metrics, ticker);
    coordinatorManager.handleHeartbeat(WORKER1_HOSTNAME);
    coordinatorManager.handleHeartbeat(WORKER2_HOSTNAME);

    int workerCount = (int) metrics.getGauges().get(CoordinatorBookKeeper.METRIC_BOOKKEEPER_LIVE_WORKER_GAUGE).getValue();
    assertEquals(workerCount, 2, "Incorrect number of workers reporting heartbeat");

    ticker.advance(workerLivenessExpiry, TimeUnit.MILLISECONDS);
    coordinatorManager.handleHeartbeat(WORKER1_HOSTNAME);

    workerCount = (int) metrics.getGauges().get(CoordinatorBookKeeper.METRIC_BOOKKEEPER_LIVE_WORKER_GAUGE).getValue();
    assertEquals(workerCount, 1, "Incorrect number of workers reporting heartbeat");
  }

  /**
   * Create the cache directories necessary for running the test.
   *
   * @param conf  The current Hadoop configuration.
   */
  private void createCacheDirectoriesForTest(Configuration conf)
  {
    try {
      CacheUtil.createCacheDirectories(conf);
    }
    catch (FileNotFoundException e) {
      fail("Could not create cache directories: " + e.getMessage());
    }
  }

  /**
   * Class to mock a {@link CoordinatorBookKeeper} and customize the ticker associated with the worker liveness cache.
   */
  private static class MockCoordinatorBookKeeper extends CoordinatorBookKeeper
  {
    public MockCoordinatorBookKeeper(Configuration conf, MetricRegistry metrics, Ticker ticker) throws FileNotFoundException
    {
      super(conf, metrics);
      super.liveWorkerCache = CacheBuilder.newBuilder()
          .expireAfterWrite(CacheConfig.getWorkerLivenessExpiry(conf), TimeUnit.MILLISECONDS)
          .ticker(ticker)
          .build();
    }
  }
}
