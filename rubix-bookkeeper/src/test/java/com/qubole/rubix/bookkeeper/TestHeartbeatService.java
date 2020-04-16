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
import com.qubole.rubix.common.metrics.BookKeeperMetrics;
import com.qubole.rubix.common.utils.TestUtil;
import com.qubole.rubix.spi.BookKeeperFactory;
import com.qubole.rubix.spi.CacheConfig;
import com.qubole.rubix.spi.RetryingPooledBookkeeperClient;
import com.qubole.rubix.spi.fop.Poolable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.shaded.transport.TSocket;
import org.apache.thrift.shaded.transport.TTransport;
import org.apache.thrift.shaded.transport.TTransportException;
import org.mockito.ArgumentMatchers;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

public class TestHeartbeatService
{
  private static final Log log = LogFactory.getLog(TestHeartbeatService.class);

  private static final String TEST_CACHE_DIR_PREFIX = TestUtil.getTestCacheDirPrefix("TestHeartbeatService");
  private static final int TEST_MAX_DISKS = 1;
  private static final int TEST_MAX_RETRIES = 5;
  private static final int TEST_RETRY_INTERVAL = 500;
  private static final String TEST_REMOTE_LOCATION = "testLocation";

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
    CacheConfig.setServiceRetryInterval(conf, TEST_RETRY_INTERVAL);
    CacheConfig.setHeartbeatInterval(conf, TEST_RETRY_INTERVAL);
    CacheConfig.setServiceMaxRetries(conf, TEST_MAX_RETRIES);
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
    CacheConfig.setMaxDisks(conf, TEST_MAX_DISKS);

    TestUtil.removeCacheParentDirectories(conf, TEST_MAX_DISKS);
  }

  /**
   * Verify that the heartbeat service correctly makes a connection using a BookKeeper client.
   *
   * @throws TTransportException if the BookKeeper client cannot be created.
   */
  @Test
  public void testHeartbeatRetryLogic_noRetriesNeeded() throws TTransportException, IOException
  {
    final BookKeeperFactory bookKeeperFactory = mock(BookKeeperFactory.class);
    when(bookKeeperFactory.createBookKeeperClient(anyString(), ArgumentMatchers.<Configuration>any())).thenReturn(
        new RetryingPooledBookkeeperClient(
            new Poolable<TTransport>(new TSocket("localhost", CacheConfig.getBookKeeperServerPort(conf), CacheConfig.getServerConnectTimeout(conf)), null, "localhost"),
            "localhost",
            conf));

    // Disable default reporters for this BookKeeper, since they will conflict with the running server.
    CacheConfig.setMetricsReporters(conf, "");
    try (BookKeeperMetrics bookKeeperMetrics = new BookKeeperMetrics(conf, new MetricRegistry())) {
      final BookKeeper bookKeeper = new CoordinatorBookKeeper(conf, bookKeeperMetrics);
      final HeartbeatService heartbeatService = new HeartbeatService(conf, new MetricRegistry(), bookKeeperFactory, bookKeeper);
    }
  }

  /**
   * Verify that the heartbeat service correctly makes a connection using a BookKeeper client after a number of retries.
   */
  @Test
  public void testHeartbeatRetryLogic_connectAfterRetries() throws IOException
  {
    BaseServerTest.stopBookKeeperServer();
    BaseServerTest.startCoordinatorBookKeeperServerWithDelay(conf, new MetricRegistry(), TEST_RETRY_INTERVAL * 2);

    // Disable default reporters for this BookKeeper, since they will conflict with the running server.
    CacheConfig.setMetricsReporters(conf, "");
    try (BookKeeperMetrics bookKeeperMetrics = new BookKeeperMetrics(conf, new MetricRegistry())) {
      final BookKeeper bookKeeper = new CoordinatorBookKeeper(conf, bookKeeperMetrics);
      final HeartbeatService heartbeatService = new HeartbeatService(conf, new MetricRegistry(), new BookKeeperFactory(), bookKeeper);
    }
  }

  /**
   * Verify that the heartbeat service no longer attempts to connect once it runs out of retry attempts.
   *
   * @throws TTransportException if the BookKeeper client cannot be created.
   */
  @Test(expectedExceptions = RuntimeException.class)
  public void testHeartbeatRetryLogic_outOfRetries() throws TTransportException, IOException
  {
    final BookKeeperFactory bookKeeperFactory = mock(BookKeeperFactory.class);
    when(bookKeeperFactory.createBookKeeperClient(anyString(), ArgumentMatchers.<Configuration>any())).thenThrow(TTransportException.class);

    // Disable default reporters for this BookKeeper, since they will conflict with the running server.
    CacheConfig.setMetricsReporters(conf, "");
    try (BookKeeperMetrics bookKeeperMetrics = new BookKeeperMetrics(conf, new MetricRegistry())) {
      final BookKeeper bookKeeper = new CoordinatorBookKeeper(conf, bookKeeperMetrics);
      final HeartbeatService heartbeatService = new HeartbeatService(conf, new MetricRegistry(), bookKeeperFactory, bookKeeper);
    }
  }

  /**
   * Verify that the validation success metrics are correctly registered when validation is enabled.
   *
   * @throws TTransportException if the BookKeeper client cannot be created.
   */
  @Test
  public void verifyValidationMetricsAreCorrectlyRegistered_validationEnabled() throws IOException, TTransportException
  {
    CacheConfig.setValidationEnabled(conf, true);

    final BookKeeperFactory bookKeeperFactory = mock(BookKeeperFactory.class);
    when(bookKeeperFactory.createBookKeeperClient(anyString(), ArgumentMatchers.<Configuration>any())).thenReturn(
        new RetryingPooledBookkeeperClient(
            new Poolable<TTransport>(new TSocket("localhost", CacheConfig.getBookKeeperServerPort(conf), CacheConfig.getServerConnectTimeout(conf)), null, "localhost"),
            "localhost",
            conf));

    final MetricRegistry metrics = new MetricRegistry();

    // Disable default reporters for this BookKeeper, since they will conflict with the running server.
    CacheConfig.setMetricsReporters(conf, "");
    try (BookKeeperMetrics bookKeeperMetrics = new BookKeeperMetrics(conf, metrics)) {
      final BookKeeper bookKeeper = new CoordinatorBookKeeper(conf, bookKeeperMetrics);
      final HeartbeatService heartbeatService = new HeartbeatService(conf, metrics, bookKeeperFactory, bookKeeper);

      assertNotNull(metrics.getGauges().get(BookKeeperMetrics.ValidationMetric.CACHING_VALIDATION_SUCCESS_GAUGE.getMetricName()), "Caching validation success metric should be registered!");
      assertNotNull(metrics.getGauges().get(BookKeeperMetrics.ValidationMetric.FILE_VALIDATION_SUCCESS_GAUGE.getMetricName()), "File validation success metric should be registered!");
    }
  }

  /**
   * Verify that the validation success metrics are not registered when validation is disabled.
   *
   * @throws TTransportException if the BookKeeper client cannot be created.
   */
  @Test
  public void verifyValidationMetricsAreCorrectlyRegistered_validationDisabled() throws IOException, TTransportException
  {
    CacheConfig.setValidationEnabled(conf, false);

    final BookKeeperFactory bookKeeperFactory = mock(BookKeeperFactory.class);
    when(bookKeeperFactory.createBookKeeperClient(anyString(), ArgumentMatchers.<Configuration>any())).thenReturn(
        new RetryingPooledBookkeeperClient(
            new Poolable<TTransport>(new TSocket("localhost", CacheConfig.getBookKeeperServerPort(conf), CacheConfig.getServerConnectTimeout(conf)), null, "localhost"),
            "localhost",
            conf));

    final MetricRegistry metrics = new MetricRegistry();

    // Disable default reporters for this BookKeeper, since they will conflict with the running server.
    CacheConfig.setMetricsReporters(conf, "");
    try (BookKeeperMetrics bookKeeperMetrics = new BookKeeperMetrics(conf, metrics)) {
      final BookKeeper bookKeeper = new CoordinatorBookKeeper(conf, bookKeeperMetrics);
      final HeartbeatService heartbeatService = new HeartbeatService(conf, metrics, bookKeeperFactory, bookKeeper);

      assertNull(metrics.getGauges().get(BookKeeperMetrics.ValidationMetric.CACHING_VALIDATION_SUCCESS_GAUGE.getMetricName()), "Caching validation success metric should not be registered!");
      assertNull(metrics.getGauges().get(BookKeeperMetrics.ValidationMetric.FILE_VALIDATION_SUCCESS_GAUGE.getMetricName()), "File validation success metric should not be registered!");
    }
  }
}
