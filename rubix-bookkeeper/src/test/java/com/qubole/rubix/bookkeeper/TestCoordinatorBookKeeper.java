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
import com.codahale.metrics.MetricRegistry;
import com.google.common.testing.FakeTicker;
import com.qubole.rubix.common.metrics.BookKeeperMetrics;
import com.qubole.rubix.common.utils.TestUtil;
import com.qubole.rubix.spi.CacheConfig;
import com.qubole.rubix.spi.thrift.HeartbeatStatus;
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
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

public class TestCoordinatorBookKeeper
{
  private static final Log log = LogFactory.getLog(TestCoordinatorBookKeeper.class);

  private static final String TEST_CACHE_DIR_PREFIX = TestUtil.getTestCacheDirPrefix("TestCoordinatorBookKeeper");
  private static final String TEST_HOSTNAME_WORKER1 = "worker1";
  private static final String TEST_HOSTNAME_WORKER2 = "worker2";
  private static final HeartbeatStatus TEST_STATUS_ALL_VALIDATED = new HeartbeatStatus(true, true);
  private static final int TEST_MAX_DISKS = 1;

  private final Configuration conf = new Configuration();
  private MetricRegistry metrics;

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
   * Verify that the health metrics are correctly registered.
   */
  @Test
  public void testWorkerHealthMetrics() throws FileNotFoundException
  {
    CacheConfig.setValidationEnabled(conf, true);

    final CoordinatorBookKeeper coordinatorBookKeeper = new CoordinatorBookKeeper(conf, metrics);
    coordinatorBookKeeper.handleHeartbeat(TEST_HOSTNAME_WORKER1, TEST_STATUS_ALL_VALIDATED);
    coordinatorBookKeeper.handleHeartbeat(TEST_HOSTNAME_WORKER2, TEST_STATUS_ALL_VALIDATED);

    long workerCount = (long) metrics.getGauges().get(BookKeeperMetrics.HealthMetric.LIVE_WORKER_GAUGE.getMetricName()).getValue();
    long cachingValidatedCount = (long) metrics.getGauges().get(BookKeeperMetrics.HealthMetric.CACHING_VALIDATED_WORKER_GAUGE.getMetricName()).getValue();
    long fileValidatedCount = (long) metrics.getGauges().get(BookKeeperMetrics.HealthMetric.FILE_VALIDATED_WORKER_GAUGE.getMetricName()).getValue();

    assertEquals(workerCount, 2, "Incorrect number of workers reporting heartbeat");
    assertEquals(cachingValidatedCount, 2, "Incorrect number of workers reporting heartbeat");
    assertEquals(fileValidatedCount, 2, "Incorrect number of workers reporting heartbeat");
  }

  /**
   * Verify that the worker health status properly expires.
   */
  @Test
  public void testWorkerHealthMetrics_healthStatusExpired() throws FileNotFoundException
  {
    final FakeTicker ticker = new FakeTicker();
    final int healthStatusExpiry = 1000; // ms
    CacheConfig.setValidationEnabled(conf, true);
    CacheConfig.setHealthStatusExpiry(conf, healthStatusExpiry);

    final CoordinatorBookKeeper coordinatorBookKeeper = new CoordinatorBookKeeper(conf, metrics, ticker);
    final Gauge liveWorkerGauge = metrics.getGauges().get(BookKeeperMetrics.HealthMetric.LIVE_WORKER_GAUGE.getMetricName());
    final Gauge cachingValidatedWorkerGauge = metrics.getGauges().get(BookKeeperMetrics.HealthMetric.CACHING_VALIDATED_WORKER_GAUGE.getMetricName());
    final Gauge fileValidatedWorkerGauge = metrics.getGauges().get(BookKeeperMetrics.HealthMetric.FILE_VALIDATED_WORKER_GAUGE.getMetricName());

    coordinatorBookKeeper.handleHeartbeat(TEST_HOSTNAME_WORKER1, TEST_STATUS_ALL_VALIDATED);
    coordinatorBookKeeper.handleHeartbeat(TEST_HOSTNAME_WORKER2, TEST_STATUS_ALL_VALIDATED);

    long workerCount = (long) liveWorkerGauge.getValue();
    long cachingValidationCount = (long) cachingValidatedWorkerGauge.getValue();
    long fileValidationCount = (long) fileValidatedWorkerGauge.getValue();
    assertEquals(workerCount, 2, "Incorrect number of workers reporting heartbeat");
    assertEquals(cachingValidationCount, 2, "Incorrect number of workers have been validated");
    assertEquals(fileValidationCount, 2, "Incorrect number of workers have been validated");

    ticker.advance(healthStatusExpiry, TimeUnit.MILLISECONDS);
    coordinatorBookKeeper.handleHeartbeat(TEST_HOSTNAME_WORKER1, TEST_STATUS_ALL_VALIDATED);

    workerCount = (long) liveWorkerGauge.getValue();
    cachingValidationCount = (long) cachingValidatedWorkerGauge.getValue();
    fileValidationCount = (long) fileValidatedWorkerGauge.getValue();
    assertEquals(workerCount, 1, "Incorrect number of workers reporting heartbeat");
    assertEquals(cachingValidationCount, 1, "Incorrect number of workers have been validated");
    assertEquals(fileValidationCount, 1, "Incorrect number of workers have been validated");
  }

  /**
   * Verify that the validated workers metrics are correctly registered when validation is enabled.
   */
  @Test
  public void testWorkerHealthMetrics_validatedWorkersMetricsRegisteredWhenValidationEnabled() throws FileNotFoundException
  {
    CacheConfig.setValidationEnabled(conf, true);
    final CoordinatorBookKeeper coordinatorBookKeeper = new CoordinatorBookKeeper(conf, metrics);

    assertNotNull(metrics.getGauges().get(BookKeeperMetrics.HealthMetric.CACHING_VALIDATED_WORKER_GAUGE.getMetricName()), "Caching-validated workers metric should be registered!");
    assertNotNull(metrics.getGauges().get(BookKeeperMetrics.HealthMetric.FILE_VALIDATED_WORKER_GAUGE.getMetricName()), "File-validated workers metric should be registered!");
  }

  /**
   * Verify that the validated workers metrics are not registered when validation is disabled.
   */
  @Test
  public void testWorkerHealthMetrics_validatedWorkersMetricsNotRegisteredWhenValidationDisabled() throws FileNotFoundException
  {
    CacheConfig.setValidationEnabled(conf, false);
    final CoordinatorBookKeeper coordinatorBookKeeper = new CoordinatorBookKeeper(conf, metrics);

    assertNull(metrics.getGauges().get(BookKeeperMetrics.HealthMetric.CACHING_VALIDATED_WORKER_GAUGE.getMetricName()), "Caching-validated workers metric should not be registered!");
    assertNull(metrics.getGauges().get(BookKeeperMetrics.HealthMetric.FILE_VALIDATED_WORKER_GAUGE.getMetricName()), "File-validated workers metric should not be registered!");
  }
}
