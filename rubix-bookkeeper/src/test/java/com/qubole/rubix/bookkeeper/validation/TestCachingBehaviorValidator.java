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
package com.qubole.rubix.bookkeeper.validation;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.Lists;
import com.qubole.rubix.bookkeeper.BookKeeper;
import com.qubole.rubix.bookkeeper.CoordinatorBookKeeper;
import com.qubole.rubix.common.metrics.BookKeeperMetrics;
import com.qubole.rubix.spi.CacheConfig;
import com.qubole.rubix.spi.thrift.BlockLocation;
import com.qubole.rubix.spi.thrift.Location;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.shaded.TException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.FileNotFoundException;
import java.util.List;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

public class TestCachingBehaviorValidator
{
  private static final Log log = LogFactory.getLog(TestCachingBehaviorValidator.class);

  private static final int TEST_VALIDATION_INTERVAL = 1000; // ms
  private static final String TEST_REMOTE_LOCATION = "testLocation";
  private static final List<BlockLocation> TEST_LOCATIONS_CACHED = Lists.newArrayList(new BlockLocation(Location.CACHED, TEST_REMOTE_LOCATION));
  private static final List<BlockLocation> TEST_LOCATIONS_LOCAL = Lists.newArrayList(new BlockLocation(Location.LOCAL, TEST_REMOTE_LOCATION));

  private final Configuration conf = new Configuration();
  private MetricRegistry metrics;

  private CachingBehaviorValidator cachingBehaviorValidator;

  @BeforeMethod
  public void setUp()
  {
    metrics = new MetricRegistry();
  }

  @AfterMethod
  public void tearDown()
  {
    conf.clear();
  }

  /**
   * Verify that the behavior of the BookKeeper caching flow is correct.
   *
   * @throws FileNotFoundException when cache directories cannot be created.
   */
  @Test
  public void testValidateCachingBehavior() throws FileNotFoundException
  {
    CacheConfig.setCachingBehaviorValidationEnabled(conf, false);
    CacheConfig.setValidationInitialDelay(conf, TEST_VALIDATION_INTERVAL);
    CacheConfig.setValidationInterval(conf, TEST_VALIDATION_INTERVAL);

    cachingBehaviorValidator = new CachingBehaviorValidator(conf, metrics, new CoordinatorBookKeeper(conf, metrics));
    cachingBehaviorValidator.runOneIteration();

    assertEquals(metrics.getGauges().get(BookKeeperMetrics.CacheMetric.METRIC_BOOKKEEPER_CACHE_BEHAVIOR_VALIDATION.getMetricName()).getValue(), 1);
  }

  /**
   * Verify that the validator reports incorrect behavior when the cache status is in the wrong initial state.
   *
   * @throws TException when file metadata cannot be fetched or refreshed.
   */
  @Test
  public void testValidateCachingBehavior_wrongInitialCacheStatus() throws TException
  {
    CacheConfig.setCachingBehaviorValidationEnabled(conf, false);
    CacheConfig.setValidationInitialDelay(conf, TEST_VALIDATION_INTERVAL);
    CacheConfig.setValidationInterval(conf, TEST_VALIDATION_INTERVAL);

    final BookKeeper bookKeeper = mock(BookKeeper.class);
    when(bookKeeper.getCacheStatus(anyString(), anyLong(), anyLong(), anyLong(), anyLong(), anyInt())).thenReturn(TEST_LOCATIONS_CACHED);
    when(bookKeeper.readData(anyString(), anyLong(), anyInt(), anyLong(), anyLong(), anyInt())).thenReturn(true);

    cachingBehaviorValidator = new CachingBehaviorValidator(conf, metrics, bookKeeper);
    cachingBehaviorValidator.runOneIteration();

    assertEquals(metrics.getGauges().get(BookKeeperMetrics.CacheMetric.METRIC_BOOKKEEPER_CACHE_BEHAVIOR_VALIDATION.getMetricName()).getValue(), 0);
  }

  /**
   * Verify that the validator reports incorrect behavior when the file data is unable to be read to cache.
   *
   * @throws TException when file metadata cannot be fetched or refreshed.
   */
  @Test
  public void testValidateCachingBehavior_dataNotRead() throws TException
  {
    CacheConfig.setCachingBehaviorValidationEnabled(conf, false);
    CacheConfig.setValidationInitialDelay(conf, TEST_VALIDATION_INTERVAL);
    CacheConfig.setValidationInterval(conf, TEST_VALIDATION_INTERVAL);

    final BookKeeper bookKeeper = mock(BookKeeper.class);
    when(bookKeeper.readData(anyString(), anyLong(), anyInt(), anyLong(), anyLong(), anyInt())).thenReturn(false);

    cachingBehaviorValidator = new CachingBehaviorValidator(conf, metrics, bookKeeper);
    cachingBehaviorValidator.runOneIteration();

    assertEquals(metrics.getGauges().get(BookKeeperMetrics.CacheMetric.METRIC_BOOKKEEPER_CACHE_BEHAVIOR_VALIDATION.getMetricName()).getValue(), 0);
  }

  /**
   * Verify that the validator reports incorrect behavior when the file data was not properly cached.
   *
   * @throws TException when file metadata cannot be fetched or refreshed.
   */
  @Test
  public void testValidateCachingBehavior_dataRead_fileNotCached() throws TException
  {
    CacheConfig.setCachingBehaviorValidationEnabled(conf, false);
    CacheConfig.setValidationInitialDelay(conf, TEST_VALIDATION_INTERVAL);
    CacheConfig.setValidationInterval(conf, TEST_VALIDATION_INTERVAL);

    final BookKeeper bookKeeper = mock(BookKeeper.class);
    when(bookKeeper.getCacheStatus(anyString(), anyLong(), anyLong(), anyLong(), anyLong(), anyInt())).thenReturn(TEST_LOCATIONS_LOCAL);
    when(bookKeeper.readData(anyString(), anyLong(), anyInt(), anyLong(), anyLong(), anyInt())).thenReturn(true);

    cachingBehaviorValidator = new CachingBehaviorValidator(conf, metrics, bookKeeper);
    cachingBehaviorValidator.runOneIteration();

    assertEquals(metrics.getGauges().get(BookKeeperMetrics.CacheMetric.METRIC_BOOKKEEPER_CACHE_BEHAVIOR_VALIDATION.getMetricName()).getValue(), 0);
  }

  /**
   * Verify that the validator reports incorrect behavior when an exception is thrown when fetching cache status.
   *
   * @throws TException when file metadata cannot be fetched or refreshed.
   */
  @Test
  public void testValidateCachingBehavior_cacheStatusException() throws TException
  {
    CacheConfig.setCachingBehaviorValidationEnabled(conf, false);
    CacheConfig.setValidationInitialDelay(conf, TEST_VALIDATION_INTERVAL);
    CacheConfig.setValidationInterval(conf, TEST_VALIDATION_INTERVAL);

    final BookKeeper bookKeeper = mock(BookKeeper.class);
    when(bookKeeper.getCacheStatus(anyString(), anyLong(), anyLong(), anyLong(), anyLong(), anyInt())).thenThrow(TException.class);

    cachingBehaviorValidator = new CachingBehaviorValidator(conf, metrics, bookKeeper);
    cachingBehaviorValidator.runOneIteration();

    assertEquals(metrics.getGauges().get(BookKeeperMetrics.CacheMetric.METRIC_BOOKKEEPER_CACHE_BEHAVIOR_VALIDATION.getMetricName()).getValue(), 0);
  }

  /**
   * Verify that the validator reports incorrect behavior when an exception is thrown when reading data.
   *
   * @throws TException when file metadata cannot be fetched or refreshed.
   */
  @Test
  public void testValidateCachingBehavior_readDataException() throws TException
  {
    CacheConfig.setCachingBehaviorValidationEnabled(conf, false);
    CacheConfig.setValidationInitialDelay(conf, TEST_VALIDATION_INTERVAL);
    CacheConfig.setValidationInterval(conf, TEST_VALIDATION_INTERVAL);

    final BookKeeper bookKeeper = mock(BookKeeper.class);
    when(bookKeeper.readData(anyString(), anyLong(), anyInt(), anyLong(), anyLong(), anyInt())).thenThrow(TException.class);

    cachingBehaviorValidator = new CachingBehaviorValidator(conf, metrics, bookKeeper);
    cachingBehaviorValidator.runOneIteration();

    assertEquals(metrics.getGauges().get(BookKeeperMetrics.CacheMetric.METRIC_BOOKKEEPER_CACHE_BEHAVIOR_VALIDATION.getMetricName()).getValue(), 0);
  }

  /**
   * Verify that cache metrics are not affected during cache behavior validation.
   *
   * @throws FileNotFoundException when cache directories cannot be created.
   */
  @Test
  public void testValidateCachingBehavior_verifyOtherMetricsUnaffected() throws FileNotFoundException
  {
    CacheConfig.setCachingBehaviorValidationEnabled(conf, false);
    CacheConfig.setValidationInitialDelay(conf, TEST_VALIDATION_INTERVAL);
    CacheConfig.setValidationInterval(conf, TEST_VALIDATION_INTERVAL);

    final BookKeeper bookKeeper = new CoordinatorBookKeeper(conf, metrics);
    assertEquals(metrics.getCounters().get(BookKeeperMetrics.CacheMetric.METRIC_BOOKKEEPER_TOTAL_REQUEST_COUNT.getMetricName()).getCount(), 0);

    cachingBehaviorValidator = new CachingBehaviorValidator(conf, metrics, bookKeeper);
    cachingBehaviorValidator.runOneIteration();

    assertEquals(metrics.getGauges().get(BookKeeperMetrics.CacheMetric.METRIC_BOOKKEEPER_CACHE_BEHAVIOR_VALIDATION.getMetricName()).getValue(), 1);
    assertEquals(metrics.getCounters().get(BookKeeperMetrics.CacheMetric.METRIC_BOOKKEEPER_TOTAL_REQUEST_COUNT.getMetricName()).getCount(), 0);
  }
}
