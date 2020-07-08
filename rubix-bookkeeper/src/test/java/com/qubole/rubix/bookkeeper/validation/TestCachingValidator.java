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
package com.qubole.rubix.bookkeeper.validation;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.Lists;
import com.qubole.rubix.bookkeeper.BookKeeper;
import com.qubole.rubix.bookkeeper.CoordinatorBookKeeper;
import com.qubole.rubix.common.metrics.BookKeeperMetrics;
import com.qubole.rubix.common.utils.TestUtil;
import com.qubole.rubix.spi.BookKeeperFactory;
import com.qubole.rubix.spi.CacheConfig;
import com.qubole.rubix.spi.RetryingPooledBookkeeperClient;
import com.qubole.rubix.spi.fop.Poolable;
import com.qubole.rubix.spi.thrift.BlockLocation;
import com.qubole.rubix.spi.thrift.CacheStatusRequest;
import com.qubole.rubix.spi.thrift.CacheStatusResponse;
import com.qubole.rubix.spi.thrift.Location;
import com.qubole.rubix.spi.thrift.ReadResponse;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.shaded.TException;
import org.apache.thrift.shaded.transport.TSocket;
import org.apache.thrift.shaded.transport.TTransport;
import org.mockito.ArgumentMatchers;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.concurrent.Executors;

import static com.qubole.rubix.spi.CacheUtil.UNKONWN_GENERATION_NUMBER;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

public class TestCachingValidator
{
  private static final Log log = LogFactory.getLog(TestCachingValidator.class);

  private static final String TEST_CACHE_DIR_PREFIX = TestUtil.getTestCacheDirPrefix("TestCachingValidator");
  private static final int TEST_MAX_DISKS = 1;
  private static final int TEST_VALIDATION_INTERVAL = 1000; // ms
  private static final String TEST_REMOTE_LOCATION = "testLocation";
  private static final CacheStatusResponse TEST_LOCATIONS_CACHED = new CacheStatusResponse(Lists.newArrayList(new BlockLocation(Location.CACHED, TEST_REMOTE_LOCATION)), UNKONWN_GENERATION_NUMBER + 1);
  private static final CacheStatusResponse TEST_LOCATIONS_LOCAL = new CacheStatusResponse(Lists.newArrayList(new BlockLocation(Location.LOCAL, TEST_REMOTE_LOCATION)), UNKONWN_GENERATION_NUMBER + 1);

  private final Configuration conf = new Configuration();

  @BeforeMethod
  public void setUp() throws IOException
  {
    CacheConfig.setCacheDataDirPrefix(conf, TEST_CACHE_DIR_PREFIX);
    CacheConfig.setMaxDisks(conf, TEST_MAX_DISKS);

    TestUtil.createCacheParentDirectories(conf, TEST_MAX_DISKS);
  }

  @AfterMethod
  public void tearDown() throws IOException
  {
    CacheConfig.setCacheDataDirPrefix(conf, TEST_CACHE_DIR_PREFIX);
    TestUtil.removeCacheParentDirectories(conf, TEST_MAX_DISKS);

    conf.clear();
  }

  /**
   * Verify that the behavior of the BookKeeper caching flow is correct.
   *
   * @throws TException when file metadata cannot be fetched or refreshed.
   * @throws FileNotFoundException when cache directories cannot be created.
   */
  @Test
  public void testValidateCachingBehavior() throws TException, IOException
  {
    try (BookKeeperMetrics bookKeeperMetrics = new BookKeeperMetrics(conf, new MetricRegistry())) {
      checkValidator(new CoordinatorBookKeeper(conf, bookKeeperMetrics), true);
    }
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
    when(bookKeeper.getCacheStatus(any(CacheStatusRequest.class))).thenReturn(TEST_LOCATIONS_CACHED);
    ReadResponse response = new ReadResponse(true, UNKONWN_GENERATION_NUMBER);
    when(bookKeeper.readData(anyString(), anyLong(), anyInt(), anyLong(), anyLong(), anyInt())).thenReturn(response);

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
    ReadResponse response = new ReadResponse(false, UNKONWN_GENERATION_NUMBER);
    when(bookKeeper.readData(anyString(), anyLong(), anyInt(), anyLong(), anyLong(), anyInt())).thenReturn(response);

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
    when(bookKeeper.getCacheStatus(any(CacheStatusRequest.class))).thenReturn(TEST_LOCATIONS_LOCAL);
    ReadResponse response = new ReadResponse(false, UNKONWN_GENERATION_NUMBER);
    when(bookKeeper.readData(anyString(), anyLong(), anyInt(), anyLong(), anyLong(), anyInt())).thenReturn(response);

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
    when(bookKeeper.getCacheStatus(any(CacheStatusRequest.class))).thenThrow(TException.class);

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
  public void testValidateCachingBehavior_verifyOtherMetricsUnaffected() throws TException, IOException
  {
    final MetricRegistry metrics = new MetricRegistry();

    try (BookKeeperMetrics bookKeeperMetrics = new BookKeeperMetrics(conf, metrics)) {
      final BookKeeper bookKeeper = new CoordinatorBookKeeper(conf, bookKeeperMetrics);

      assertEquals(metrics.getCounters().get(BookKeeperMetrics.CacheMetric.TOTAL_REQUEST_COUNT.getMetricName()).getCount(), 0);

      checkValidator(bookKeeper, true);

      assertEquals(metrics.getCounters().get(BookKeeperMetrics.CacheMetric.TOTAL_REQUEST_COUNT.getMetricName()).getCount(), 0);
    }
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
    final BookKeeperFactory bookKeeperFactory = mock(BookKeeperFactory.class);
    when(bookKeeperFactory.createBookKeeperClient(anyString(), ArgumentMatchers.<Configuration>any())).thenReturn(
            new RetryingPooledBookkeeperClient(
                    new Poolable<TTransport>(new TSocket("localhost", CacheConfig.getBookKeeperServerPort(conf), CacheConfig.getServerConnectTimeout(conf)), null, "localhost"),
                    null,
                    conf));

    CachingValidator validator = new CachingValidator(conf, bookKeeper, Executors.newSingleThreadScheduledExecutor());
    assertEquals(validator.validateCachingBehavior(), expectedResult);
  }
}
