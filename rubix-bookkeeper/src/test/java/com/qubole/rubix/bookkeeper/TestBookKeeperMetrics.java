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
import com.qubole.rubix.bookkeeper.test.BookKeeperTestUtils;
import com.qubole.rubix.spi.CacheConfig;
import com.qubole.rubix.spi.ClusterType;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.shaded.TException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.FileNotFoundException;
import java.io.IOException;

import static org.testng.Assert.assertEquals;

public class TestBookKeeperMetrics
{
  private static final Log log = LogFactory.getLog(TestBookKeeperMetrics.class);

  private static final String TEST_CACHE_DIR_PREFIX = BookKeeperTestUtils.getTestCacheDirPrefix("TestBookKeeperMetrics");
  private static final int TEST_BLOCK_SIZE = 100;
  private static final int TEST_MAX_DISKS = 1;

  private final Configuration conf = new Configuration();
  private final MetricRegistry metrics = new MetricRegistry();

  private BookKeeper bookKeeper;

  @BeforeClass
  public void initializeCacheDirectories() throws IOException
  {
    CacheConfig.setCacheDataDirPrefix(conf, TEST_CACHE_DIR_PREFIX);

    BookKeeperTestUtils.createCacheParentDirectories(conf, TEST_MAX_DISKS);
  }

  @BeforeMethod
  public void setUpForTest() throws FileNotFoundException
  {
    CacheConfig.setCacheDataDirPrefix(conf, TEST_CACHE_DIR_PREFIX);
    CacheConfig.setBlockSize(conf, TEST_BLOCK_SIZE);

    bookKeeper = new CoordinatorBookKeeper(conf, metrics);
  }

  @AfterMethod
  public void tearDownForTest()
  {
    conf.clear();
    metrics.removeMatching(MetricFilter.ALL);
  }

  @AfterClass
  public void cleanCacheDirectories() throws IOException
  {
    CacheConfig.setCacheDataDirPrefix(conf, TEST_CACHE_DIR_PREFIX);

    BookKeeperTestUtils.removeCacheParentDirectories(conf, TEST_MAX_DISKS);
  }

  /**
   * Verify that the metric representing total block hits is correctly registered & incremented.
   *
   * @throws TException when file metadata cannot be fetched or refreshed.
   */
  @Test
  public void verifyBlockHitsMetricIsReported() throws TException
  {
    final String remotePath = "/tmp/testPath";
    final long lastModified = 1514764800; // 2018-01-01T00:00:00
    final long fileLength = 5000;
    final long startBlock = 20;
    final long endBlock = 23;
    final long totalRequests = endBlock - startBlock;

    assertEquals(metrics.getCounters().get(BookKeeper.METRIC_BOOKKEEPER_LOCAL_CACHE_COUNT).getCount(), 0);
    bookKeeper.getCacheStatus(remotePath, fileLength, lastModified, startBlock, endBlock, ClusterType.TEST_CLUSTER_MANAGER.ordinal());
    assertEquals(metrics.getCounters().get(BookKeeper.METRIC_BOOKKEEPER_LOCAL_CACHE_COUNT).getCount(), totalRequests);
  }
}
