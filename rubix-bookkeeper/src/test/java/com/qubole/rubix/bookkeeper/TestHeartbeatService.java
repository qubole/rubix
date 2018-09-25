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
import com.qubole.rubix.common.metrics.BookKeeperMetrics;
import com.qubole.rubix.spi.BookKeeperFactory;
import com.qubole.rubix.spi.CacheConfig;
import com.qubole.rubix.spi.RetryingBookkeeperClient;
import com.qubole.rubix.spi.thrift.BlockLocation;
import com.qubole.rubix.spi.thrift.Location;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.shaded.TException;
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
import java.util.Collections;
import java.util.List;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

public class TestHeartbeatService
{
  private static final Log log = LogFactory.getLog(TestWorkerBookKeeper.class);

  private static final String TEST_CACHE_DIR_PREFIX = TestUtil.getTestCacheDirPrefix("TestWorkerBookKeeper");
  private static final int TEST_MAX_DISKS = 1;
  private static final int TEST_MAX_RETRIES = 5;
  private static final int TEST_RETRY_INTERVAL = 500;
  private static final int TEST_VALIDATION_INTERVAL = 1000; // ms
  private static final String TEST_REMOTE_LOCATION = "testLocation";
  private static final List<BlockLocation> TEST_LOCATIONS_CACHED = Collections.singletonList(new BlockLocation(Location.CACHED, TEST_REMOTE_LOCATION));
  private static final List<BlockLocation> TEST_LOCATIONS_LOCAL = Collections.singletonList(new BlockLocation(Location.LOCAL, TEST_REMOTE_LOCATION));

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
    CacheConfig.setServiceRetryInterval(conf, TEST_RETRY_INTERVAL);
    CacheConfig.setHeartbeatInterval(conf, TEST_RETRY_INTERVAL);
    CacheConfig.setServiceMaxRetries(conf, TEST_MAX_RETRIES);

    CacheConfig.setOnMaster(conf, true);
    BaseServerTest.startBookKeeperServer(conf, new MetricRegistry());
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
   * Verify that the heartbeat service correctly makes a connection using a BookKeeper client.
   *
   * @throws TTransportException if the BookKeeper client cannot be created.
   */
  @Test
  public void testHeartbeatRetryLogic_noRetriesNeeded() throws TTransportException, FileNotFoundException
  {
    final BookKeeperFactory bookKeeperFactory = mock(BookKeeperFactory.class);
    when(bookKeeperFactory.createBookKeeperClient(anyString(), ArgumentMatchers.<Configuration>any())).thenReturn(
        new RetryingBookkeeperClient(
            new TSocket("localhost", CacheConfig.getServerPort(conf), CacheConfig.getClientTimeout(conf)),
            CacheConfig.getMaxRetries(conf)));

    final BookKeeper bookKeeper = new CoordinatorBookKeeper(conf, new MetricRegistry());
    final HeartbeatService heartbeatService = new HeartbeatService(conf, bookKeeperFactory, bookKeeper);
  }

  /**
   * Verify that the heartbeat service correctly makes a connection using a BookKeeper client after a number of retries.
   */
  @Test
  public void testHeartbeatRetryLogic_connectAfterRetries() throws FileNotFoundException
  {
    BaseServerTest.stopBookKeeperServer();
    BaseServerTest.startBookKeeperServerWithDelay(conf, new MetricRegistry(), TEST_RETRY_INTERVAL * 2);

    final BookKeeper bookKeeper = new CoordinatorBookKeeper(conf, new MetricRegistry());
    final HeartbeatService heartbeatService = new HeartbeatService(conf, new BookKeeperFactory(), bookKeeper);
  }

  /**
   * Verify that the heartbeat service no longer attempts to connect once it runs out of retry attempts.
   *
   * @throws TTransportException if the BookKeeper client cannot be created.
   */
  @Test(expectedExceptions = RuntimeException.class)
  public void testHeartbeatRetryLogic_outOfRetries() throws TTransportException, FileNotFoundException
  {
    final BookKeeperFactory bookKeeperFactory = mock(BookKeeperFactory.class);
    when(bookKeeperFactory.createBookKeeperClient(anyString(), ArgumentMatchers.<Configuration>any())).thenThrow(TTransportException.class);

    final BookKeeper bookKeeper = new CoordinatorBookKeeper(conf, new MetricRegistry());
    final HeartbeatService heartbeatService = new HeartbeatService(conf, bookKeeperFactory, bookKeeper);
  }

  /**
   * Verify that the behavior of the BookKeeper caching flow is correct.
   *
   * @throws TException when file metadata cannot be fetched or refreshed.
   * @throws FileNotFoundException when cache directories cannot be created.
   */
  @Test
  public void testValidateCachingBehavior() throws TException, FileNotFoundException
  {
    checkValidator(new CoordinatorBookKeeper(conf, new MetricRegistry()), true);
  }

  /**
   * Verify that the validator reports incorrect behavior when the cache status is in the wrong initial state.
   *
   * @throws TException when file metadata cannot be fetched or refreshed.
   */
  @Test
  public void testValidateCachingBehavior_wrongInitialCacheStatus() throws TException
  {
    final BookKeeper bookKeeper = mock(BookKeeper.class);
    when(bookKeeper.getCacheStatus(anyString(), anyLong(), anyLong(), anyLong(), anyLong(), anyInt())).thenReturn(TEST_LOCATIONS_CACHED);
    when(bookKeeper.readData(anyString(), anyLong(), anyInt(), anyLong(), anyLong(), anyInt())).thenReturn(true);

    checkValidator(bookKeeper, false);
  }

  /**
   * Verify that the validator reports incorrect behavior when the file data is unable to be read to cache.
   *
   * @throws TException when file metadata cannot be fetched or refreshed.
   */
  @Test
  public void testValidateCachingBehavior_dataNotRead() throws TException
  {
    final BookKeeper bookKeeper = mock(BookKeeper.class);
    when(bookKeeper.readData(anyString(), anyLong(), anyInt(), anyLong(), anyLong(), anyInt())).thenReturn(false);

    checkValidator(bookKeeper, false);
  }

  /**
   * Verify that the validator reports incorrect behavior when the file data was not properly cached.
   *
   * @throws TException when file metadata cannot be fetched or refreshed.
   */
  @Test
  public void testValidateCachingBehavior_dataRead_fileNotCached() throws TException
  {
    final BookKeeper bookKeeper = mock(BookKeeper.class);
    when(bookKeeper.getCacheStatus(anyString(), anyLong(), anyLong(), anyLong(), anyLong(), anyInt())).thenReturn(TEST_LOCATIONS_LOCAL);
    when(bookKeeper.readData(anyString(), anyLong(), anyInt(), anyLong(), anyLong(), anyInt())).thenReturn(true);

    checkValidator(bookKeeper, false);
  }

  /**
   * Verify that the validator reports incorrect behavior when an exception is thrown when fetching cache status.
   *
   * @throws TException when file metadata cannot be fetched or refreshed.
   */
  @Test
  public void testValidateCachingBehavior_cacheStatusException() throws TException
  {
    final BookKeeper bookKeeper = mock(BookKeeper.class);
    when(bookKeeper.getCacheStatus(anyString(), anyLong(), anyLong(), anyLong(), anyLong(), anyInt())).thenThrow(TException.class);

    checkValidator(bookKeeper, false);
  }

  /**
   * Verify that the validator reports incorrect behavior when an exception is thrown when reading data.
   *
   * @throws TException when file metadata cannot be fetched or refreshed.
   */
  @Test
  public void testValidateCachingBehavior_readDataException() throws TException
  {
    final BookKeeper bookKeeper = mock(BookKeeper.class);
    when(bookKeeper.readData(anyString(), anyLong(), anyInt(), anyLong(), anyLong(), anyInt())).thenThrow(TException.class);

    checkValidator(bookKeeper, false);
  }

  /**
   * Verify that cache metrics are not affected during cache behavior validation.
   *
   * @throws TException when file metadata cannot be fetched or refreshed.
   * @throws FileNotFoundException when cache directories cannot be created.
   */
  @Test
  public void testValidateCachingBehavior_verifyOtherMetricsUnaffected() throws TException, FileNotFoundException
  {
    CacheConfig.setCachingBehaviorValidationEnabled(conf, false);
    final MetricRegistry metrics = new MetricRegistry();
    final BookKeeper bookKeeper = new CoordinatorBookKeeper(conf, metrics);

    assertEquals(metrics.getCounters().get(BookKeeperMetrics.CacheMetric.TOTAL_REQUEST_COUNT.getMetricName()).getCount(), 0);

    checkValidator(bookKeeper, true);

    assertEquals(metrics.getCounters().get(BookKeeperMetrics.CacheMetric.TOTAL_REQUEST_COUNT.getMetricName()).getCount(), 0);
  }

  /**
   * Run the caching behavior validator and verify the result.
   *
   * @param bookKeeper      The BookKeeper client used for testing the caching behaviour.
   * @param expectedResult  The result expected from validation.
   * @throws TException when file metadata cannot be fetched or refreshed.
   */
  private void checkValidator(BookKeeper bookKeeper, boolean expectedResult) throws TException
  {
    CacheConfig.setValidationInitialDelay(conf, TEST_VALIDATION_INTERVAL);
    CacheConfig.setValidationInterval(conf, TEST_VALIDATION_INTERVAL);

    final BookKeeperFactory bookKeeperFactory = mock(BookKeeperFactory.class);
    when(bookKeeperFactory.createBookKeeperClient(anyString(), ArgumentMatchers.<Configuration>any())).thenReturn(
        new RetryingBookkeeperClient(
            new TSocket("localhost", CacheConfig.getServerPort(conf), CacheConfig.getClientTimeout(conf)),
            CacheConfig.getMaxRetries(conf)));

    HeartbeatService heartbeatService = new HeartbeatService(conf, bookKeeperFactory, bookKeeper);
    assertEquals(heartbeatService.validateCachingBehavior(), expectedResult);
  }
}
