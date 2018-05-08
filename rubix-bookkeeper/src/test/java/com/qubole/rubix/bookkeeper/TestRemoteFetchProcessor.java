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

import com.google.common.collect.RangeSet;
import com.qubole.rubix.core.utils.DataGen;
import com.qubole.rubix.core.utils.DeleteFileVisitor;
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
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.ConcurrentMap;

import static org.testng.Assert.assertTrue;

/**
 * Created by Abhishek on 3/6/18.
 */
public class TestRemoteFetchProcessor
{
  private Configuration conf;
  private static final Log log = LogFactory.getLog(TestRemoteFetchProcessor.class.getName());

  int blockSize = 100;
  private static final String testDirectoryPrefix = System.getProperty("java.io.tmpdir") + "/TestRemoteFetchProcessor/";
  String backendFileName = testDirectoryPrefix + "backendFile";
  Path backendPath = new Path("file:///" + backendFileName.substring(1));
  private static final String testDirectory = testDirectoryPrefix + "dir0";

  @BeforeClass
  public static void setupClass() throws IOException
  {
  }

  @AfterClass
  public static void tearDownClass() throws IOException
  {
  }

  @BeforeMethod
  public void setUp() throws Exception
  {
    conf = new Configuration();
    CacheConfig.setCacheDataDirPrefix(conf, testDirectoryPrefix + "dir");
    CacheConfig.setBlockSize(conf, blockSize);
    CacheConfig.setMaxDisks(conf, 1);
    Files.createDirectories(Paths.get(testDirectory, CacheConfig.getCacheDataDirSuffix(conf)));
  }

  @AfterMethod
  public void tearDown() throws Exception
  {
    log.info("Deleting files in " + testDirectory);
    Files.walkFileTree(Paths.get(testDirectory), new DeleteFileVisitor());
    Files.deleteIfExists(Paths.get(testDirectory));
  }

  @Test
  public void testMergeRequests() throws Exception
  {
    CacheConfig.setRemoteFetchProcessInterval(conf, 2000);
    RemoteFetchProcessor processor = new RemoteFetchProcessor(conf);

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
    for (int i = 0; i < 100; i++) {
      String path = "File--" + (i % 10);
      processor.addToProcessQueue(path, i, i + 10, 100, 1000);
    }

    contextMap = processor.mergeRequests(System.currentTimeMillis() + 3000);

    expected = 10;
    assertTrue(expected == contextMap.size(),
        "Merge didn't work. Expecting Number of File Requests " + expected + " Got : " + contextMap.size());

    log.info("Merge Test 3 when requests non overlapping set from one file");
    for (int i = 0; i < 300; i += 30) {
      String path = "File--1";
      processor.addToProcessQueue(path, i, 10, 100, 1000);
    }
    contextMap = processor.mergeRequests(System.currentTimeMillis() + 3000);

    expected = 10;
    int result = ((RangeSet<Long>) contextMap.get("File--1").getRanges()).asRanges().size();
    assertTrue(expected == result,
        "Merge didn't work. Expecting Number Ranges in file " + expected + " Got : " + result);

    log.info("Merge Test 4 when requests overlapping set from one file");
    for (int i = 0; i < 300; i += 30) {
      String path = "File--1";
      processor.addToProcessQueue(path, i, 50, 100, 1000);
    }

    contextMap = processor.mergeRequests(System.currentTimeMillis() + 3000);

    expected = 1;
    result = ((RangeSet<Long>) contextMap.get("File--1").getRanges()).asRanges().size();
    assertTrue(expected == result,
        "Merge didn't work. Expecting Number Ranges in file " + expected + " Got : " + result);
  }

  @Test
  public void testProcessRequestOverlappingSet() throws Exception
  {
    DataGen.populateFile(backendFileName);
    File file = new File(backendFileName);
    Path backendPath = new Path("file:///" + backendFileName);

    CacheConfig.setRemoteFetchProcessInterval(conf, 2000);
    RemoteFetchProcessor processsor = new RemoteFetchProcessor(conf);

    processsor.addToProcessQueue(backendPath.toString(), 0, 100, file.length(), (long) 10000);
    processsor.addToProcessQueue(backendPath.toString(), 50, 100, file.length(), (long) 10000);
    processsor.addToProcessQueue(backendPath.toString(), 100, 100, file.length(), (long) 10000);
    processsor.addToProcessQueue(backendPath.toString(), 150, 100, file.length(), (long) 10000);
    processsor.addToProcessQueue(backendPath.toString(), 200, 100, file.length(), (long) 10000);
    processsor.addToProcessQueue(backendPath.toString(), 250, 100, file.length(), (long) 10000);
    processsor.addToProcessQueue(backendPath.toString(), 300, 100, file.length(), (long) 10000);
    processsor.addToProcessQueue(backendPath.toString(), 350, 100, file.length(), (long) 10000);
    processsor.addToProcessQueue(backendPath.toString(), 400, 100, file.length(), (long) 10000);

    processsor.processRequest(System.currentTimeMillis() + 2000);

    String downloadedFile = CacheUtil.getLocalPath(backendPath.toString(), conf);
    String resultString = new String(DataGen.readBytesFromFile(downloadedFile, 0, 500));

    String expected = DataGen.generateContent().substring(0, 500);
    assertTrue(expected.length() == resultString.length(),
        "Downloaded data length didn't match Expected : " + expected.length() + " Got : " + resultString.length());
    assertTrue(expected.equals(resultString),
        "Downloaded data didn't match Expected : " + expected + " Got : " + resultString);
  }

  @Test
  public void testProcessRequestNonOverlappingSet() throws Exception
  {
    DataGen.populateFile(backendFileName);
    File file = new File(backendFileName);
    Path backendPath = new Path("file:///" + backendFileName);

    CacheConfig.setRemoteFetchProcessInterval(conf, 2000);
    RemoteFetchProcessor processsor = new RemoteFetchProcessor(conf);

    processsor.addToProcessQueue(backendPath.toString(), 0, 100, file.length(), (long) 10000);
    processsor.addToProcessQueue(backendPath.toString(), 200, 100, file.length(), (long) 10000);
    processsor.addToProcessQueue(backendPath.toString(), 400, 100, file.length(), (long) 10000);
    processsor.addToProcessQueue(backendPath.toString(), 600, 100, file.length(), (long) 10000);
    processsor.addToProcessQueue(backendPath.toString(), 800, 100, file.length(), (long) 10000);
    processsor.addToProcessQueue(backendPath.toString(), 1000, 100, file.length(), (long) 10000);

    processsor.processRequest(System.currentTimeMillis() + 2000);

    String downloadedFile = CacheUtil.getLocalPath(backendPath.toString(), conf);

    String resultString = null;
    String expected = null;
    String content = DataGen.generateContent(1);

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
