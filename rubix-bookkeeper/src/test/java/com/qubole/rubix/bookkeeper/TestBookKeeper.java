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
import com.qubole.rubix.core.utils.DeleteFileVisitor;
import com.qubole.rubix.core.utils.DummyClusterManager;
import com.qubole.rubix.spi.CacheConfig;
import com.qubole.rubix.spi.ClusterManager;
import com.qubole.rubix.spi.ClusterType;
import org.apache.hadoop.conf.Configuration;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.nio.file.Files;
import java.nio.file.Paths;

import static org.testng.Assert.assertTrue;

/**
 * Created by Abhishek on 6/15/18.
 */
public class TestBookKeeper
{
  private MetricRegistry metrics;
  private Configuration conf;

  @BeforeMethod
  public void setUp() throws Exception
  {
    conf = new Configuration();
    metrics = new MetricRegistry();

    // Set configuration values for testing
    CacheConfig.setCacheDataDirPrefix(conf, "/tmp/media/ephemeral");
    CacheConfig.setMaxDisks(conf, 1);

    // Create cache directories
    Files.createDirectories(Paths.get(CacheConfig.getCacheDirPrefixList(conf)));
    for (int i = 0; i < CacheConfig.getCacheMaxDisks(conf); i++) {
      Files.createDirectories(Paths.get(CacheConfig.getCacheDirPrefixList(conf) + i));
    }
  }

  @AfterMethod
  public void tearDown() throws Exception
  {
    for (int i = 0; i < CacheConfig.getCacheMaxDisks(conf); i++) {
      Files.walkFileTree(Paths.get(CacheConfig.getCacheDirPrefixList(conf) + i), new DeleteFileVisitor());
      Files.deleteIfExists(Paths.get(CacheConfig.getCacheDirPrefixList(conf) + i));
    }
  }

  @Test
  public void testGetClusterManagerValidInstance() throws Exception
  {
    ClusterType type = ClusterType.TEST_CLUSTER_MANAGER;
    BookKeeper bookKeeper = new CoordinatorBookKeeper(conf, metrics);
    ClusterManager manager = bookKeeper.getClusterManagerInstance(type, conf);

    assertTrue(manager instanceof DummyClusterManager, " Didn't initialize the correct cluster manager class." +
        " Expected : " + DummyClusterManager.class + " Got : " + manager.getClass());
  }

  @Test(expectedExceptions = ClusterManagerInitilizationException.class)
  public void testGetClusterManagerInValidInstance() throws Exception
  {
    ClusterType type = ClusterType.TEST_CLUSTER_MANAGER;
    CacheConfig.setDummyClusterManager(conf, "com.qubole.rubix.core.DoesNotExistClusterManager");
    BookKeeper bookKeeper = new CoordinatorBookKeeper(conf, metrics);

    ClusterManager manager = bookKeeper.getClusterManagerInstance(type, conf);
  }
}
