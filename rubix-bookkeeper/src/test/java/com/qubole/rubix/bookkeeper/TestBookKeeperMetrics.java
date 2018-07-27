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

import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.testing.FakeTicker;
import com.qubole.rubix.bookkeeper.test.BookKeeperTestUtils;
import com.qubole.rubix.bookkeeper.utils.DiskUtils;
import com.qubole.rubix.core.utils.DataGen;
import com.qubole.rubix.spi.CacheConfig;
import com.qubole.rubix.spi.CacheUtil;
import com.qubole.rubix.spi.ClusterType;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.shaded.TException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertEquals;

public class TestBookKeeperMetrics
{
  private static final Log log = LogFactory.getLog(TestBookKeeperMetrics.class);

  private static final String TEST_CACHE_DIR_PREFIX = BookKeeperTestUtils.getTestCacheDirPrefix("TestBookKeeperMetrics");
  private static final int TEST_BLOCK_SIZE = 100;
  private static final int TEST_MAX_DISKS = 1;
  private static final String TEST_REMOTE_PATH = "/tmp/testPath";
  private static final long TEST_LAST_MODIFIED = 1514764800; // 2018-01-01T00:00:00
  private static final long TEST_FILE_LENGTH = 5000;
  private static final long TEST_START_BLOCK = 20;
  private static final long TEST_END_BLOCK = 23;

  private final Configuration conf = new Configuration();
  private final MetricRegistry metrics = new MetricRegistry();

  private BookKeeper bookKeeper;

  @BeforeMethod
  public void setUp() throws IOException
  {
    CacheConfig.setCacheDataDirPrefix(conf, TEST_CACHE_DIR_PREFIX);
    CacheConfig.setBlockSize(conf, TEST_BLOCK_SIZE);

    BookKeeperTestUtils.createCacheParentDirectories(conf, TEST_MAX_DISKS);

    bookKeeper = new CoordinatorBookKeeper(conf, metrics);
    bookKeeper.clusterManager = null;
  }

  @AfterMethod
  public void tearDown() throws IOException
  {
    BookKeeperTestUtils.removeCacheParentDirectories(conf, TEST_MAX_DISKS);

    conf.clear();
    metrics.removeMatching(MetricFilter.ALL);
  }

  /**
   * Verify that the metric representing total requests is correctly registered & incremented.
   *
   * @throws TException when file metadata cannot be fetched or refreshed.
   */
  @Test
  public void verifyTotalRequestMetricIsReported() throws TException
  {
    final long totalRequests = TEST_END_BLOCK - TEST_START_BLOCK;

    assertEquals(metrics.getCounters().get(BookKeeper.METRIC_BOOKKEEPER_TOTAL_REQUEST_COUNT).getCount(), 0);

    bookKeeper.getCacheStatus(TEST_REMOTE_PATH, TEST_FILE_LENGTH, TEST_LAST_MODIFIED, TEST_START_BLOCK, TEST_END_BLOCK, ClusterType.TEST_CLUSTER_MANAGER.ordinal());

    assertEquals(metrics.getCounters().get(BookKeeper.METRIC_BOOKKEEPER_TOTAL_REQUEST_COUNT).getCount(), totalRequests);
  }

  /**
   * Verify that the metric representing total remote requests is correctly registered & incremented.
   *
   * @throws TException when file metadata cannot be fetched or refreshed.
   */
  @Test
  public void verifyRemoteRequestMetricIsReported() throws TException
  {
    final long totalRequests = TEST_END_BLOCK - TEST_START_BLOCK;

    assertEquals(metrics.getCounters().get(BookKeeper.METRIC_BOOKKEEPER_REMOTE_REQUEST_COUNT).getCount(), 0);

    bookKeeper.getCacheStatus(TEST_REMOTE_PATH, TEST_FILE_LENGTH, TEST_LAST_MODIFIED, TEST_START_BLOCK, TEST_END_BLOCK, ClusterType.TEST_CLUSTER_MANAGER.ordinal());

    assertEquals(metrics.getCounters().get(BookKeeper.METRIC_BOOKKEEPER_REMOTE_REQUEST_COUNT).getCount(), totalRequests);
  }

  /**
   * Verify that the metric representing total local requests is correctly registered & incremented.
   *
   * @throws TException when file metadata cannot be fetched or refreshed.
   */
  @Test
  public void verifyLocalRequestMetricIsReported() throws TException
  {
    final long totalRequests = TEST_END_BLOCK - TEST_START_BLOCK;

    assertEquals(metrics.getCounters().get(BookKeeper.METRIC_BOOKKEEPER_CACHE_REQUEST_COUNT).getCount(), 0);

    bookKeeper.getCacheStatus(TEST_REMOTE_PATH, TEST_FILE_LENGTH, TEST_LAST_MODIFIED, TEST_START_BLOCK, TEST_END_BLOCK, ClusterType.TEST_CLUSTER_MANAGER.ordinal());
    bookKeeper.setAllCached(TEST_REMOTE_PATH, TEST_FILE_LENGTH, TEST_LAST_MODIFIED, TEST_START_BLOCK, TEST_END_BLOCK);
    bookKeeper.getCacheStatus(TEST_REMOTE_PATH, TEST_FILE_LENGTH, TEST_LAST_MODIFIED, TEST_START_BLOCK, TEST_END_BLOCK, ClusterType.TEST_CLUSTER_MANAGER.ordinal());

    assertEquals(metrics.getCounters().get(BookKeeper.METRIC_BOOKKEEPER_CACHE_REQUEST_COUNT).getCount(), totalRequests);
  }

  /**
   * Verify that the metric representing total non-local requests is correctly registered & incremented.
   *
   * @throws TException when file metadata cannot be fetched or refreshed.
   */
  @Test
  public void verifyNonlocalRequestMetricIsReported() throws TException
  {
    final long totalRequests = TEST_END_BLOCK - TEST_START_BLOCK;

    assertEquals(metrics.getCounters().get(BookKeeper.METRIC_BOOKKEEPER_NONLOCAL_REQUEST_COUNT).getCount(), 0);
    assertEquals(metrics.getCounters().get(BookKeeper.METRIC_BOOKKEEPER_CACHE_REQUEST_COUNT).getCount(), 0);

    bookKeeper.getCacheStatus(TEST_REMOTE_PATH, TEST_FILE_LENGTH, TEST_LAST_MODIFIED, TEST_START_BLOCK, TEST_END_BLOCK, ClusterType.TEST_CLUSTER_MANAGER_MULTINODE.ordinal());

    assertEquals(metrics.getCounters().get(BookKeeper.METRIC_BOOKKEEPER_NONLOCAL_REQUEST_COUNT).getCount(), totalRequests);
    assertEquals(metrics.getCounters().get(BookKeeper.METRIC_BOOKKEEPER_CACHE_REQUEST_COUNT).getCount(), 0);
  }

  /**
   * Verify that the metric representing the current cache size is correctly registered & reports expected values.
   *
   * @throws IOException if an I/O error occurs when interacting with the cache.
   */
  @Test
  public void verifyCacheSizeMetricIsReported() throws IOException, TException
  {
    final String remotePathWithScheme = "file://" + TEST_REMOTE_PATH;
    final int readOffset = 0;
    final int readLength = 100;

    // Since the value returned from a gauge metric is an object rather than a primitive, boxing is required here to properly compare the values.
    assertEquals(metrics.getGauges().get(BookKeeper.METRIC_BOOKKEEPER_CACHE_SIZE_GAUGE).getValue(), 0);

    DataGen.populateFile(TEST_REMOTE_PATH);
    bookKeeper.readData(remotePathWithScheme, readOffset, readLength, TEST_FILE_LENGTH, TEST_LAST_MODIFIED, ClusterType.TEST_CLUSTER_MANAGER.ordinal());

    final long mdSize = FileUtils.sizeOf(new File(CacheUtil.getMetadataFilePath(TEST_REMOTE_PATH, conf)));
    final int totalCacheSize = DiskUtils.bytesToMB(readLength + mdSize);
    assertEquals(metrics.getGauges().get(BookKeeper.METRIC_BOOKKEEPER_CACHE_SIZE_GAUGE).getValue(), totalCacheSize);
  }

  /**
   * Verify that the metric representing total cache evictions is correctly registered & incremented.
   *
   * @throws TException when file metadata cannot be fetched or refreshed.
   * @throws FileNotFoundException when cache directories cannot be created.
   */
  @Test
  public void verifyCacheEvictionMetricIsReported() throws TException, FileNotFoundException
  {
    final FakeTicker ticker = new FakeTicker();
    CacheConfig.setCacheDataExpirationAfterWrite(conf, 1000);
    metrics.removeMatching(MetricFilter.ALL);
    bookKeeper = new CoordinatorBookKeeper(conf, metrics, ticker);

    assertEquals(metrics.getCounters().get(BookKeeper.METRIC_BOOKKEEPER_CACHE_EVICTION_COUNT).getCount(), 0);

    bookKeeper.getCacheStatus(TEST_REMOTE_PATH, TEST_FILE_LENGTH, TEST_LAST_MODIFIED, TEST_START_BLOCK, TEST_END_BLOCK, ClusterType.TEST_CLUSTER_MANAGER.ordinal());
    ticker.advance(30000, TimeUnit.MILLISECONDS);
    bookKeeper.fileMetadataCache.cleanUp();

    assertEquals(metrics.getCounters().get(BookKeeper.METRIC_BOOKKEEPER_CACHE_EVICTION_COUNT).getCount(), 1);
  }

  /**
   * Verify that the metrics representing cache hits & misses are correctly registered and report expected values.
   *
   * @throws TException when file metadata cannot be fetched or refreshed.
   */
  @Test
  public void verifyCacheHitAndMissMetricsAreReported() throws TException
  {
    assertEquals(metrics.getGauges().get(BookKeeper.METRIC_BOOKKEEPER_CACHE_HIT_RATE_GAUGE).getValue(), Double.NaN);
    assertEquals(metrics.getGauges().get(BookKeeper.METRIC_BOOKKEEPER_CACHE_MISS_RATE_GAUGE).getValue(), Double.NaN);

    bookKeeper.getCacheStatus(TEST_REMOTE_PATH, TEST_FILE_LENGTH, TEST_LAST_MODIFIED, TEST_START_BLOCK, TEST_END_BLOCK, ClusterType.TEST_CLUSTER_MANAGER.ordinal());

    assertEquals(metrics.getGauges().get(BookKeeper.METRIC_BOOKKEEPER_CACHE_HIT_RATE_GAUGE).getValue(), 0.0);
    assertEquals(metrics.getGauges().get(BookKeeper.METRIC_BOOKKEEPER_CACHE_MISS_RATE_GAUGE).getValue(), 1.0);

    bookKeeper.setAllCached(TEST_REMOTE_PATH, TEST_FILE_LENGTH, TEST_LAST_MODIFIED, TEST_START_BLOCK, TEST_END_BLOCK);
    bookKeeper.getCacheStatus(TEST_REMOTE_PATH, TEST_FILE_LENGTH, TEST_LAST_MODIFIED, TEST_START_BLOCK, TEST_END_BLOCK, ClusterType.TEST_CLUSTER_MANAGER.ordinal());

    assertEquals(metrics.getGauges().get(BookKeeper.METRIC_BOOKKEEPER_CACHE_HIT_RATE_GAUGE).getValue(), 0.5);
    assertEquals(metrics.getGauges().get(BookKeeper.METRIC_BOOKKEEPER_CACHE_MISS_RATE_GAUGE).getValue(), 0.5);
  }
}
