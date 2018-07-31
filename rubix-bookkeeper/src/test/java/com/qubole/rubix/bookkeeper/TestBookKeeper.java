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
import com.qubole.rubix.core.ClusterManagerInitilizationException;
import com.qubole.rubix.core.utils.DataGen;
import com.qubole.rubix.core.utils.DummyClusterManager;
import com.qubole.rubix.hadoop2.Hadoop2ClusterManager;
import com.qubole.rubix.presto.PrestoClusterManager;
import com.qubole.rubix.spi.CacheConfig;
import com.qubole.rubix.spi.ClusterManager;
import com.qubole.rubix.spi.ClusterType;
import com.qubole.rubix.spi.thrift.FileInfo;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertTrue;

/**
 * Created by Abhishek on 6/15/18.
 */
public class TestBookKeeper
{
  private static final Log log = LogFactory.getLog(TestBookKeeper.class);

  private static final String TEST_CACHE_DIR_PREFIX = BookKeeperTestUtils.getTestCacheDirPrefix("TestBookKeeper");
  private static final String TEST_DNE_CLUSTER_MANAGER = "com.qubole.rubix.core.DoesNotExistClusterManager";
  private static final int TEST_MAX_DISKS = 1;
  private static final String BACKEND_FILE_NAME = "backendFile";

  private final Configuration conf = new Configuration();
  private final MetricRegistry metrics = new MetricRegistry();

  @BeforeClass
  public void setUpForClass() throws Exception
  {
    CacheConfig.setCacheDataDirPrefix(conf, TEST_CACHE_DIR_PREFIX);

    BookKeeperTestUtils.createCacheParentDirectories(conf, TEST_MAX_DISKS);
  }

  @BeforeMethod
  public void setUp()
  {
    CacheConfig.setCacheDataDirPrefix(conf, TEST_CACHE_DIR_PREFIX);
  }

  @AfterMethod
  public void tearDown() throws Exception
  {
    conf.clear();
    metrics.removeMatching(MetricFilter.ALL);
  }

  @AfterClass
  public void tearDownForClass() throws Exception
  {
    CacheConfig.setCacheDataDirPrefix(conf, TEST_CACHE_DIR_PREFIX);

    BookKeeperTestUtils.removeCacheParentDirectories(conf, TEST_MAX_DISKS);
  }

  @Test
  public void testGetDummyClusterManagerValidInstance() throws Exception
  {
    ClusterType type = ClusterType.TEST_CLUSTER_MANAGER;
    BookKeeper bookKeeper = new CoordinatorBookKeeper(conf, metrics);
    ClusterManager manager = bookKeeper.getClusterManagerInstance(type, conf);

    assertTrue(manager instanceof DummyClusterManager, " Didn't initialize the correct cluster manager class." +
        " Expected : " + DummyClusterManager.class + " Got : " + manager.getClass());
  }

  @Test(expectedExceptions = ClusterManagerInitilizationException.class)
  public void testGetDummyClusterManagerInValidInstance() throws Exception
  {
    ClusterType type = ClusterType.TEST_CLUSTER_MANAGER;
    CacheConfig.setDummyClusterManager(conf, TEST_DNE_CLUSTER_MANAGER);
    BookKeeper bookKeeper = new CoordinatorBookKeeper(conf, metrics);

    ClusterManager manager = bookKeeper.getClusterManagerInstance(type, conf);
  }

  @Test
  public void testGetHadoop2ClusterManagerValidInstance() throws Exception
  {
    ClusterType type = ClusterType.HADOOP2_CLUSTER_MANAGER;
    BookKeeper bookKeeper = new CoordinatorBookKeeper(conf, metrics);
    ClusterManager manager = bookKeeper.getClusterManagerInstance(type, conf);

    assertTrue(manager instanceof Hadoop2ClusterManager, " Didn't initialize the correct cluster manager class." +
        " Expected : " + Hadoop2ClusterManager.class + " Got : " + manager.getClass());
  }

  @Test(expectedExceptions = ClusterManagerInitilizationException.class)
  public void testGetHadoop2ClusterManagerInValidInstance() throws Exception
  {
    ClusterType type = ClusterType.HADOOP2_CLUSTER_MANAGER;
    CacheConfig.setHadoopClusterManager(conf, TEST_DNE_CLUSTER_MANAGER);
    BookKeeper bookKeeper = new CoordinatorBookKeeper(conf, metrics);

    ClusterManager manager = bookKeeper.getClusterManagerInstance(type, conf);
  }

  @Test
  public void testGetPrestoClusterManagerValidInstance() throws Exception
  {
    ClusterType type = ClusterType.PRESTO_CLUSTER_MANAGER;
    BookKeeper bookKeeper = new CoordinatorBookKeeper(conf, metrics);
    ClusterManager manager = bookKeeper.getClusterManagerInstance(type, conf);

    assertTrue(manager instanceof PrestoClusterManager, " Didn't initialize the correct cluster manager class." +
        " Expected : " + PrestoClusterManager.class + " Got : " + manager.getClass());
  }

  @Test(expectedExceptions = ClusterManagerInitilizationException.class)
  public void testGetPrestoClusterManagerInValidInstance() throws Exception
  {
    ClusterType type = ClusterType.PRESTO_CLUSTER_MANAGER;
    CacheConfig.setPrestoClusterManager(conf, TEST_DNE_CLUSTER_MANAGER);
    BookKeeper bookKeeper = new CoordinatorBookKeeper(conf, metrics);

    ClusterManager manager = bookKeeper.getClusterManagerInstance(type, conf);
  }

  @Test
  public void testGetFileInfoWithInvalidationEnabled() throws Exception
  {
    Path backendFilePath = new Path(BookKeeperTestUtils.getDefaultTestDirectoryPath(conf), BACKEND_FILE_NAME);
    DataGen.populateFile(backendFilePath.toString());
    int expectedFileSize = DataGen.generateContent(1).length();

    CacheConfig.setFileStalenessCheck(conf, true);

    BookKeeper bookKeeper = new CoordinatorBookKeeper(conf, metrics);
    FileInfo info = bookKeeper.getFileInfo(backendFilePath.toString());

    assertTrue(info.getFileSize() == expectedFileSize, "FileSize was not equal to the expected value." +
        " Got FileSize: " + info.getFileSize() + " Expected Value : " + expectedFileSize);

    //Rewrite the file with half the data
    DataGen.populateFile(backendFilePath.toString(), 2);

    expectedFileSize = DataGen.generateContent(2).length();

    info = bookKeeper.getFileInfo(backendFilePath.toString());
    assertTrue(info.getFileSize() == expectedFileSize, "FileSize was not equal to the expected value." +
        " Got FileSize: " + info.getFileSize() + " Expected Value : " + expectedFileSize);
  }

  @Test
  public void testGetFileInfoWithInvalidationDisabled() throws Exception
  {
    Path backendFilePath = new Path(BookKeeperTestUtils.getDefaultTestDirectoryPath(conf), BACKEND_FILE_NAME);
    DataGen.populateFile(backendFilePath.toString());
    int expectedFileSize = DataGen.generateContent(1).length();

    CacheConfig.setFileStalenessCheck(conf, false);

    BookKeeper bookKeeper = new CoordinatorBookKeeper(conf, metrics);
    FileInfo info = bookKeeper.getFileInfo(backendFilePath.toString());

    assertTrue(info.getFileSize() == expectedFileSize, "FileSize was not equal to the expected value." +
        " Got FileSize: " + info.getFileSize() + " Expected Value : " + expectedFileSize);

    //Rewrite the file with half the data
    DataGen.populateFile(backendFilePath.toString(), 2);

    info = bookKeeper.getFileInfo(backendFilePath.toString());
    assertTrue(info.getFileSize() == expectedFileSize, "FileSize was not equal to the expected value." +
        " Got FileSize: " + info.getFileSize() + " Expected Value : " + expectedFileSize);
  }

  @Test
  public void testGetFileInfoWithInvalidationDisabledWithCacheExpired() throws Exception
  {
    Path backendFilePath = new Path(BookKeeperTestUtils.getDefaultTestDirectoryPath(conf), BACKEND_FILE_NAME);
    DataGen.populateFile(backendFilePath.toString());
    int expectedFileSize = DataGen.generateContent(1).length();

    CacheConfig.setFileStalenessCheck(conf, false);
    CacheConfig.setStaleFileInfoExpiryPeriod(conf, 5);

    FakeTicker ticker = new FakeTicker();

    BookKeeper bookKeeper = new CoordinatorBookKeeper(conf, metrics, ticker);
    FileInfo info = bookKeeper.getFileInfo(backendFilePath.toString());

    assertTrue(info.getFileSize() == expectedFileSize, "FileSize was not equal to the expected value." +
        " Got FileSize: " + info.getFileSize() + " Expected Value : " + expectedFileSize);

    //Rewrite the file with half the data
    DataGen.populateFile(backendFilePath.toString(), 2);

    info = bookKeeper.getFileInfo(backendFilePath.toString());
    assertTrue(info.getFileSize() == expectedFileSize, "FileSize was not equal to the expected value." +
        " Got FileSize: " + info.getFileSize() + " Expected Value : " + expectedFileSize);

    // Advance the ticker to 5 sec
    ticker.advance(5, TimeUnit.SECONDS);

    expectedFileSize = DataGen.generateContent(2).length();
    info = bookKeeper.getFileInfo(backendFilePath.toString());
    assertTrue(info.getFileSize() == expectedFileSize, "FileSize was not equal to the expected value." +
        " Got FileSize: " + info.getFileSize() + " Expected Value : " + expectedFileSize);
  }
}
