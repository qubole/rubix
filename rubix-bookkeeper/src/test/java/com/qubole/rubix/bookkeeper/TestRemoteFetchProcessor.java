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
import java.util.concurrent.ConcurrentMap;

import static org.testng.Assert.assertTrue;

/**
 * Created by Abhishek on 3/6/18.
 */
public class TestRemoteFetchProcessor
{
  private static final Log log = LogFactory.getLog(TestRemoteFetchProcessor.class);

  private static final String TEST_CACHE_DIR_PREFIX = TestUtil.getTestCacheDirPrefix("TestRemoteFetchProcessor");
  private static final String TEST_BACKEND_FILE_NAME = TEST_CACHE_DIR_PREFIX + "backendFile";
  private static final int TEST_BLOCK_SIZE = 100;
  private static final int TEST_MAX_DISKS = 1;
  private static final int TEST_REMOTE_FETCH_PROCESS_INTERVAL = 2000;
  private BookKeeper bookKeeper;

  private final Configuration conf = new Configuration();

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
    MetricRegistry metrics = new MetricRegistry();
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
  public void testMergeRequests() throws Exception
  {
    CacheConfig.setRemoteFetchProcessInterval(conf, TEST_REMOTE_FETCH_PROCESS_INTERVAL);
    final RemoteFetchProcessor processor = new RemoteFetchProcessor(bookKeeper, conf);

    log.info("Merge Test 1 when requests are all from different file");
    for (int i = 0; i < 100; i++) {
      String path = "File--" + i;
      processor.addToProcessQueue(path, i, i + 10, 100, 1000);
    }

    ConcurrentMap<String, DownloadRequestContext> contextMap = processor.mergeRequests(System.currentTimeMillis() + 3000);

    int expected = 100;
    assertTrue(expected == contextMap.size(),
        "Merge didn't work. Expecting Number of File Requests " + expected + " Got : " + contextMap.size());

    contextMap.clear();

    log.info("Merge Test 2 when requests are from a set of files");
    String path;
    for (int i = 0; i < 100; i++) {
      path = "File--" + (i % 10);
      processor.addToProcessQueue(path, i, i + 10, 100, 1000);
    }

    contextMap = processor.mergeRequests(System.currentTimeMillis() + 3000);

    expected = 10;
    assertTrue(expected == contextMap.size(),
        "Merge didn't work. Expecting Number of File Requests " + expected + " Got : " + contextMap.size());

    log.info("Merge Test 3 when requests non overlapping set from one file");
    for (int i = 0; i < 300; i += 30) {
      path = "File--1";
      processor.addToProcessQueue(path, i, 10, 100, 1000);
    }
    contextMap = processor.mergeRequests(System.currentTimeMillis() + 3000);

    expected = 10;
    int result = (contextMap.get("File--1").getRanges()).asRanges().size();
    assertTrue(expected == result,
        "Merge didn't work. Expecting Number Ranges in file " + expected + " Got : " + result);

    log.info("Merge Test 4 when requests overlapping set from one file");
    for (int i = 0; i < 300; i += 30) {
      path = "File--1";
      processor.addToProcessQueue(path, i, 50, 100, 1000);
    }

    contextMap = processor.mergeRequests(System.currentTimeMillis() + 3000);

    expected = 1;
    result = (contextMap.get("File--1").getRanges()).asRanges().size();
    assertTrue(expected == result,
        "Merge didn't work. Expecting Number Ranges in file " + expected + " Got : " + result);
  }

  @Test
  public void testProcessRequestOverlappingSet() throws Exception
  {
    DataGen.populateFile(TEST_BACKEND_FILE_NAME);
    final File file = new File(TEST_BACKEND_FILE_NAME);
    final Path backendPath = new Path("file:///" + TEST_BACKEND_FILE_NAME);
    final int offsetStep = 50;
    final int maxOffset = 400;

    CacheConfig.setRemoteFetchProcessInterval(conf, TEST_REMOTE_FETCH_PROCESS_INTERVAL);
    final RemoteFetchProcessor processsor = new RemoteFetchProcessor(bookKeeper, conf);

    for (int offset = 0; offset <= maxOffset; offset += offsetStep) {
      processsor.addToProcessQueue(backendPath.toString(), offset, 100, file.length(), (long) 10000);
    }
    processsor.processRequest(System.currentTimeMillis() + TEST_REMOTE_FETCH_PROCESS_INTERVAL);

    final String downloadedFile = CacheUtil.getLocalPath(backendPath.toString(), conf);
    final String resultString = new String(DataGen.readBytesFromFile(downloadedFile, 0, 500));

    final String expected = DataGen.generateContent().substring(0, 500);
    assertTrue(expected.length() == resultString.length(),
        "Downloaded data length didn't match Expected : " + expected.length() + " Got : " + resultString.length());
    assertTrue(expected.equals(resultString),
        "Downloaded data didn't match Expected : " + expected + " Got : " + resultString);
  }

  @Test
  public void testProcessRequestNonOverlappingSet() throws Exception
  {
    DataGen.populateFile(TEST_BACKEND_FILE_NAME);
    final File file = new File(TEST_BACKEND_FILE_NAME);
    final Path backendPath = new Path("file:///" + TEST_BACKEND_FILE_NAME);
    final int offsetStep = 200;
    final int maxOffset = 1000;

    CacheConfig.setRemoteFetchProcessInterval(conf, TEST_REMOTE_FETCH_PROCESS_INTERVAL);
    final RemoteFetchProcessor processsor = new RemoteFetchProcessor(bookKeeper, conf);

    for (int offset = 0; offset <= maxOffset; offset += offsetStep) {
      processsor.addToProcessQueue(backendPath.toString(), offset, 100, file.length(), (long) 10000);
    }
    processsor.processRequest(System.currentTimeMillis() + TEST_REMOTE_FETCH_PROCESS_INTERVAL);

    final String downloadedFile = CacheUtil.getLocalPath(backendPath.toString(), conf);

    String resultString = null;
    String expected = null;
    final String content = DataGen.generateContent(1);

    for (int i = 0; i < 1200; i += 200) {
      resultString = new String(DataGen.readBytesFromFile(downloadedFile, i, 100));
      expected = content.substring(i, i + 100);
      assertTrue(resultString.length() == 100,
          "Downloaded data length didn't match Expected : " + 100 + " Got : " + resultString.length());
      assertTrue(expected.equals(resultString),
          "Downloaded data didn't match Expected : " + expected + " Got : " + resultString);
    }
  }
}
