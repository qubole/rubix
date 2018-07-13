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

import com.qubole.rubix.bookkeeper.test.BookKeeperTestUtils;
import com.qubole.rubix.core.FileDownloadRequestChain;
import com.qubole.rubix.spi.CacheConfig;
import com.qubole.rubix.spi.CacheUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

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

  private static final String TEST_CACHE_DIR_PREFIX = BookKeeperTestUtils.getTestCacheDirPrefix("TestFileDownloader");
  private static final int TEST_MAX_DISKS = 1;

  private final Configuration conf = new Configuration();

  @BeforeMethod
  public void setUp() throws Exception
  {
    CacheConfig.setCacheDataDirPrefix(conf, TEST_CACHE_DIR_PREFIX);
    CacheConfig.setMaxDisks(conf, TEST_MAX_DISKS);

    BookKeeperTestUtils.createCacheParentDirectories(TEST_CACHE_DIR_PREFIX, TEST_MAX_DISKS);
    CacheUtil.createCacheDirectories(conf);
  }

  @AfterMethod
  public void tearDown() throws Exception
  {
    conf.clear();

    BookKeeperTestUtils.removeCacheParentDirectories(TEST_CACHE_DIR_PREFIX);
  }

  @Test
  public void testGetFileDownloadRequestChains() throws Exception
  {
    final String remoteFilePath1 = "file:///Files/file-1";
    final String remoteFilePath2 = "file:///Files/file-2";
    ConcurrentMap<String, DownloadRequestContext> contextMap = new ConcurrentHashMap<>();

    DownloadRequestContext context = new DownloadRequestContext(remoteFilePath1, 1000, 1000);
    contextMap.put(remoteFilePath1, context);
    context.addDownloadRange(100, 300);
    context.addDownloadRange(250, 400);
    context.addDownloadRange(500, 800);

    context = new DownloadRequestContext(remoteFilePath2, 1000, 1000);
    contextMap.put(remoteFilePath2, context);
    context.addDownloadRange(100, 200);
    context.addDownloadRange(500, 800);

    FileDownloader downloader = new FileDownloader(conf);
    List<FileDownloadRequestChain> requestChains = downloader.getFileDownloadRequestChains(contextMap);

    assertTrue(requestChains.size() == 2,
        "Wrong Number of Request Chains. Expected = 2 Got = " + requestChains.size());

    FileDownloadRequestChain request1 = requestChains.get(0);
    assertTrue(request1.getReadRequests().size() == 2,
        "Wrong Number of Requests For File-1. Expected = 2 Got = " + request1.getReadRequests().size());

    FileDownloadRequestChain request2 = requestChains.get(1);
    assertTrue(request2.getReadRequests().size() == 2,
        "Wrong Number of Requests For File-2. Expected = 2 Got = " + request2.getReadRequests().size());
  }
}
