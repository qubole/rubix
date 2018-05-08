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

import com.qubole.rubix.core.FileDownloadRequestChain;
import com.qubole.rubix.core.utils.DeleteFileVisitor;
import com.qubole.rubix.spi.CacheConfig;
import org.apache.hadoop.conf.Configuration;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.testng.Assert.assertTrue;

/**
 * Created by Abhishek on 3/12/18.
 */
public class TestFileDownloader
{
  private static final String testDirectoryPrefix = System.getProperty("java.io.tmpdir") + "/TestFileDownloader/";
  private static final String testDirectory = testDirectoryPrefix + "dir0";
  private Configuration conf;

  @BeforeMethod
  public void setUp() throws Exception
  {
    conf = new Configuration();
    CacheConfig.setCacheDataDirPrefix(conf, testDirectoryPrefix + "dir");
    CacheConfig.setMaxDisks(conf, 1);
    Files.createDirectories(Paths.get(testDirectory, CacheConfig.getCacheDataDirSuffix(conf)));
  }

  @AfterMethod
  public void tearDown() throws Exception
  {
    Files.walkFileTree(Paths.get(testDirectory), new DeleteFileVisitor());
    Files.deleteIfExists(Paths.get(testDirectory));
  }

  @Test
  public void testGetFileDownloadRequestChains() throws Exception
  {
    ConcurrentMap<String, DownloadRequestContext> contextMap = new ConcurrentHashMap<String, DownloadRequestContext>();
    DownloadRequestContext context = new DownloadRequestContext("file:///Files/file-1", 1000, 1000);
    contextMap.put("file:///Files/file-1", context);
    context.addDownloadRange(100, 300);
    context.addDownloadRange(250, 400);
    context.addDownloadRange(500, 800);

    context = new DownloadRequestContext("file:///Files/file-2", 1000, 1000);
    contextMap.put("file:///Files/file-2", context);
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
