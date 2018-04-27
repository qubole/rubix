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
package com.qubole.rubix.bookkeeper;

import com.codahale.metrics.MetricRegistry;
import com.qubole.rubix.spi.CacheConfig;
import com.qubole.rubix.spi.ClusterType;
import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.shaded.TException;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class TestBookKeeperMetrics
{
  private static final int BLOCK_SIZE = 100;

  private final MetricRegistry metrics = new MetricRegistry();
  private final Configuration conf = new Configuration();

  private BookKeeper bookKeeper;

  @BeforeClass
  public void setUp()
  {
    conf.setInt(CacheConfig.blockSizeConf, BLOCK_SIZE);
    bookKeeper = new BookKeeper(conf, metrics);
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

    assertEquals(metrics.getCounters().get(BookKeeper.METRIC_TOTAL_BLOCK_HITS).getCount(), 0);
    bookKeeper.getCacheStatus(remotePath, fileLength, lastModified, startBlock, endBlock, ClusterType.TEST_CLUSTER_MANAGER.ordinal());
    assertEquals(metrics.getCounters().get(BookKeeper.METRIC_TOTAL_BLOCK_HITS).getCount(), totalRequests);
  }
}
