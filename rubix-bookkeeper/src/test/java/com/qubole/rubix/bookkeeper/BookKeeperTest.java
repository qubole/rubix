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
import com.qubole.rubix.core.ClusterManagerInitilizationException;
import com.qubole.rubix.core.utils.DummyClusterManager;
import com.qubole.rubix.spi.CacheConfig;
import com.qubole.rubix.spi.ClusterManager;
import com.qubole.rubix.spi.ClusterType;
import org.apache.hadoop.conf.Configuration;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertTrue;

/**
 * Created by Abhishek on 6/15/18.
 */
public class BookKeeperTest
{
  @BeforeMethod
  public void setUp() throws Exception
  {
  }

  @AfterMethod
  public void tearDown() throws Exception
  {
  }

  @Test
  public void testGetClusterManagerValidInstance() throws Exception
  {
    Configuration conf = new Configuration();
    ClusterType type = ClusterType.TEST_CLUSTER_MANAGER;
    MetricRegistry metrics = new MetricRegistry();

    BookKeeper bookKeeper = new CoordinatorBookKeeper(conf, metrics);

    ClusterManager manager = bookKeeper.getClusterManagerInstance(type, conf);

    assertTrue(manager instanceof DummyClusterManager, " Didn't initialize the correct cluster manager class." +
        " Expected : " + DummyClusterManager.class + " Got : " + manager.getClass());
  }

  @Test(expectedExceptions = ClusterManagerInitilizationException.class)
  public void testGetClusterManagerInValidInstance() throws Exception
  {
    Configuration conf = new Configuration();
    ClusterType type = ClusterType.TEST_CLUSTER_MANAGER;
    MetricRegistry metrics = new MetricRegistry();

    CacheConfig.setDummyClusterManager(conf, "com.qubole.rubix.core.DoesNotExistClusterManager");
    BookKeeper bookKeeper = new CoordinatorBookKeeper(conf, metrics);

    ClusterManager manager = bookKeeper.getClusterManagerInstance(type, conf);
  }
}
