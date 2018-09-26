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
import com.qubole.rubix.core.utils.DataGen;
import com.qubole.rubix.spi.CacheConfig;
import com.qubole.rubix.spi.CacheUtil;
import com.qubole.rubix.spi.ClusterType;
import com.qubole.rubix.spi.thrift.BlockLocation;
import com.qubole.rubix.spi.thrift.Location;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.testng.Assert.assertTrue;

/**
 * Created by Abhishek on 3/12/18.
 */
public class TestFileDownloader
{
  private static final Log log = LogFactory.getLog(TestFileDownloader.class);

  private static final String TEST_CACHE_DIR_PREFIX = TestUtil.getTestCacheDirPrefix("TestFileDownloader");
  private static final String TEST_BACKEND_FILE_NAME = TEST_CACHE_DIR_PREFIX + "backendFile";
  private static final int TEST_MAX_DISKS = 1;

  private final Configuration conf = new Configuration();
  private BookKeeper bookKeeper;
  private MetricRegistry metrics;

  @BeforeClass
  public void setUpForClass() throws Exception
  {
    CacheConfig.setCacheDataDirPrefix(conf, TEST_CACHE_DIR_PREFIX);

    TestUtil.createCacheParentDirectories(conf, TEST_MAX_DISKS);
    CacheUtil.createCacheDirectories(conf);
  }

  @BeforeMethod
  public void setUp() throws IOException
  {
    CacheConfig.setCacheDataDirPrefix(conf, TEST_CACHE_DIR_PREFIX);
    CacheConfig.setBlockSize(conf, 200);
    metrics = new MetricRegistry();
    bookKeeper = new CoordinatorBookKeeper(conf, metrics);
  }

  @AfterMethod
  public void tearDown() throws Exception
  {
    conf.clear();
  }

  @AfterClass
  public void tearDownForClass() throws Exception
  {
    CacheConfig.setCacheDataDirPrefix(conf, TEST_CACHE_DIR_PREFIX);

    TestUtil.removeCacheParentDirectories(conf, TEST_MAX_DISKS);
  }

  @Test
  public void testGetFileDownloadRequestChains() throws Exception
  {
    final String remoteFilePath1 = "file:///Files/file-1";
    final String remoteFilePath2 = "file:///Files/file-2";
    final ConcurrentMap<String, DownloadRequestContext> contextMap = new ConcurrentHashMap<>();

    DownloadRequestContext context = new DownloadRequestContext(remoteFilePath1, 1000, 1000);
    contextMap.put(remoteFilePath1, context);
    context.addDownloadRange(100, 300);
    context.addDownloadRange(250, 400);
    context.addDownloadRange(500, 800);

    context = new DownloadRequestContext(remoteFilePath2, 1000, 1000);
    contextMap.put(remoteFilePath2, context);
    context.addDownloadRange(100, 200);
    context.addDownloadRange(500, 800);

    final FileDownloader downloader = new FileDownloader(bookKeeper, conf);
    final List<FileDownloadRequestChain> requestChains = downloader.getFileDownloadRequestChains(contextMap);

    assertTrue(requestChains.size() == 2,
        "Wrong Number of Request Chains. Expected = 2 Got = " + requestChains.size());

    final FileDownloadRequestChain request1 = requestChains.get(0);
    assertTrue(request1.getReadRequests().size() == 2,
        "Wrong Number of Requests For File-1. Expected = 2 Got = " + request1.getReadRequests().size());

    final FileDownloadRequestChain request2 = requestChains.get(1);
    assertTrue(request2.getReadRequests().size() == 2,
        "Wrong Number of Requests For File-2. Expected = 2 Got = " + request2.getReadRequests().size());
  }

  @Test
  public void testProcessDownloadRequests() throws Exception
  {
    DataGen.populateFile(TEST_BACKEND_FILE_NAME);
    final File file = new File(TEST_BACKEND_FILE_NAME);
    final Path backendPath = new Path("file:///" + TEST_BACKEND_FILE_NAME);
    final ConcurrentMap<String, DownloadRequestContext> contextMap = new ConcurrentHashMap<>();

    List<BlockLocation> cacheStatus = bookKeeper.getCacheStatus(backendPath.toString(), file.length(), 1000, 0, 5, ClusterType.TEST_CLUSTER_MANAGER.ordinal());

    DownloadRequestContext context = new DownloadRequestContext(backendPath.toString(), file.length(), 1000);
    contextMap.put(backendPath.toString(), context);
    context.addDownloadRange(100, 300);
    context.addDownloadRange(250, 400);
    context.addDownloadRange(500, 800);

    final FileDownloader downloader = new FileDownloader(bookKeeper, conf);
    final List<FileDownloadRequestChain> requestChains = downloader.getFileDownloadRequestChains(contextMap);

    int dataDownloaded = downloader.processDownloadRequests(requestChains);

    int expectedDownloadedDataSize = 600;
    assertTrue(expectedDownloadedDataSize == dataDownloaded, "Download size didn't match");

    cacheStatus = bookKeeper.getCacheStatus(backendPath.toString(), file.length(), 1000, 0, 5, ClusterType.TEST_CLUSTER_MANAGER.ordinal());
    int i = 0;
    for (i = 0; i < 4; i++) {
      assertTrue(cacheStatus.get(i).getLocation() == Location.CACHED, "Data is not cached");
    }
    assertTrue(cacheStatus.get(i).getLocation() == Location.LOCAL, "Data is cached");
  }
}
