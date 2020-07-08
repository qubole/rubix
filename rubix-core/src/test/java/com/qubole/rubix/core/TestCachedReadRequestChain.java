/**
 * Copyright (c) 2019. Qubole Inc
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
package com.qubole.rubix.core;

import com.qubole.rubix.common.utils.DataGen;
import com.qubole.rubix.common.utils.TestUtil;
import com.qubole.rubix.spi.BookKeeperFactory;
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
import java.nio.charset.Charset;

import static com.qubole.rubix.spi.CacheUtil.UNKONWN_GENERATION_NUMBER;
import static org.testng.Assert.assertTrue;

/**
 * Created by stagra on 15/1/16.
 */
public class TestCachedReadRequestChain
{
  private static final Log log = LogFactory.getLog(TestCachedReadRequestChain.class);
  private static final String TEST_CACHE_DIR_PREFIX = System.getProperty("java.io.tmpdir") + "/TestCachedReadRequestChainFile/";
  private static final int TEST_MAX_DISKS = 1;
  private static final String TEST_BACKEND_FILE = TEST_CACHE_DIR_PREFIX + "backendFile";

  Path backendFilePath = new Path("file:///" + TEST_BACKEND_FILE.substring(1));
  File backendFile;
  final Configuration conf = new Configuration();
  BookKeeperFactory factory;

  @BeforeClass
  public void setUpForClass() throws IOException
  {
    CacheConfig.setCacheDataDirPrefix(conf, TEST_CACHE_DIR_PREFIX);
    CacheConfig.setMaxDisks(conf, TEST_MAX_DISKS);
    TestUtil.createCacheParentDirectories(conf, TEST_MAX_DISKS);
    CacheUtil.createCacheDirectories(conf);
  }

  @AfterClass
  public void tearDownForClass() throws IOException
  {
    CacheConfig.setCacheDataDirPrefix(conf, TEST_CACHE_DIR_PREFIX);
    TestUtil.removeCacheParentDirectories(conf, TEST_MAX_DISKS);
  }

  @BeforeMethod
  public void setup() throws IOException
  {
    CacheConfig.setCacheDataDirPrefix(conf, TEST_CACHE_DIR_PREFIX);
    CacheConfig.setMaxDisks(conf, TEST_MAX_DISKS);

    // Populate Backend File
    DataGen.populateFile(TEST_BACKEND_FILE);
    backendFile = new File(TEST_BACKEND_FILE);

    // Populate Cached File
    String cachedLocalFile = CacheUtil.getLocalPath(backendFilePath.toString(), conf, UNKONWN_GENERATION_NUMBER + 1);
    DataGen.populateFile(cachedLocalFile);

    factory = new BookKeeperFactory();
  }

  @AfterMethod
  public void cleanup() throws IOException
  {
    File localFile = new File(CacheUtil.getLocalPath(backendFilePath.toString(), conf, UNKONWN_GENERATION_NUMBER + 1));
    localFile.delete();

    backendFile.delete();

    conf.clear();
  }

  @Test
  public void testCachedRead_WithFileCached() throws IOException
  {
    byte[] buffer = new byte[1000];
    CachedReadRequestChain cachedReadRequestChain = getCachedReadRequestChain(buffer);

    cachedReadRequestChain.lock();
    long readSize = cachedReadRequestChain.call();

    ReadRequestChainStats stats = cachedReadRequestChain.getStats();

    assertTrue(readSize == 1000, "Wrong amount of data read " + readSize);
    String output = new String(buffer, Charset.defaultCharset());
    String expectedOutput = DataGen.getExpectedOutput(readSize);
    assertTrue(expectedOutput.equals(output), "Wrong data read, expected\n" + expectedOutput + "\nBut got\n" + output);
    assertTrue(stats.getCachedDataRead() == readSize, "All data should be read from cache");
    assertTrue(stats.getDirectDataRead() == 0, "No data should be read from object store");
  }

  @Test
  public void testCachedRead_WithNoLocalCachedFile() throws IOException
  {
    byte[] buffer = new byte[1000];
    String localCachedFile = CacheUtil.getLocalPath(backendFilePath.toString(), conf, UNKONWN_GENERATION_NUMBER + 1);

    CachedReadRequestChain cachedReadRequestChain = getCachedReadRequestChain(buffer);

    File localFile = new File(localCachedFile);
    localFile.delete();

    cachedReadRequestChain.lock();
    long readSize = cachedReadRequestChain.call();

    ReadRequestChainStats stats = cachedReadRequestChain.getStats();

    assertTrue(readSize == 1000, "Wrong amount of data read " + readSize);
    String output = new String(buffer, Charset.defaultCharset());
    String expectedOutput = DataGen.getExpectedOutput(readSize);
    assertTrue(expectedOutput.equals(output), "Wrong data read, expected\n" + expectedOutput + "\nBut got\n" + output);
    assertTrue(stats.getCachedDataRead() == 0, "No data should be read from cache");
    assertTrue(stats.getDirectDataRead() == readSize, "All data should be read from object store");
  }

  @Test
  public void testCachedRead_WithCorruptedLocalCachedFile_1() throws IOException
  {
    byte[] buffer = new byte[1000];
    String localCachedFile = CacheUtil.getLocalPath(backendFilePath.toString(), conf, UNKONWN_GENERATION_NUMBER + 1);

    CachedReadRequestChain cachedReadRequestChain = getCachedReadRequestChain(buffer);

    DataGen.truncateFile(localCachedFile, 1000);

    cachedReadRequestChain.lock();
    long readSize = cachedReadRequestChain.call();

    ReadRequestChainStats stats = cachedReadRequestChain.getStats();

    assertTrue(readSize == 1000, "Wrong amount of data read " + readSize);
    String output = new String(buffer, Charset.defaultCharset());
    String expectedOutput = DataGen.getExpectedOutput(readSize);
    assertTrue(expectedOutput.equals(output), "Wrong data read, expected\n" + expectedOutput + "\nBut got\n" + output);
    assertTrue(stats.getCachedDataRead() == 0, "No data should be read from cache");
    assertTrue(stats.getDirectDataRead() == 1000, "Data read from object store didn't match");
  }

  @Test
  public void testCachedRead_WithCorruptedLocalCachedFile_2() throws IOException
  {
    byte[] buffer = new byte[1000];
    String localCachedFile = CacheUtil.getLocalPath(backendFilePath.toString(), conf, UNKONWN_GENERATION_NUMBER + 1);

    CachedReadRequestChain cachedReadRequestChain = getCachedReadRequestChain(buffer);

    DataGen.truncateFile(localCachedFile, 1800);

    cachedReadRequestChain.lock();
    long readSize = cachedReadRequestChain.call();

    ReadRequestChainStats stats = cachedReadRequestChain.getStats();

    assertTrue(readSize == 1000, "Wrong amount of data read " + readSize);
    String output = new String(buffer, Charset.defaultCharset());
    String expectedOutput = DataGen.getExpectedOutput(readSize);
    assertTrue(expectedOutput.equals(output), "Wrong data read, expected\n" + expectedOutput + "\nBut got\n" + output);
    assertTrue(stats.getCachedDataRead() == 0, "No data should be read from cache");
    assertTrue(stats.getDirectDataRead() == 1000, "Data read from object store didn't match");
  }

  private CachedReadRequestChain getCachedReadRequestChain(byte[] buffer) throws IOException
  {
    MockCachingFileSystem fs = new MockCachingFileSystem();
    fs.initialize(backendFilePath.toUri(), conf);

    String localCachedFile = CacheUtil.getLocalPath(backendFilePath.toString(), conf, UNKONWN_GENERATION_NUMBER + 1);
    ReadRequest[] readRequests = getReadRequests(buffer);

    CachedReadRequestChain cachedReadRequestChain = new CachedReadRequestChain(fs.getRemoteFileSystem(), backendFilePath.toString(), conf, factory, UNKONWN_GENERATION_NUMBER + 1);
    for (ReadRequest rr : readRequests) {
      cachedReadRequestChain.addReadRequest(rr);
    }
    return cachedReadRequestChain;
  }

  private ReadRequest[] getReadRequests(byte[] buffer)
  {
    return new ReadRequest[]{
        new ReadRequest(0, 100, 0, 100, buffer, 0, backendFile.length()),
        new ReadRequest(200, 300, 200, 300, buffer, 100, backendFile.length()),
        new ReadRequest(400, 500, 400, 500, buffer, 200, backendFile.length()),
        new ReadRequest(600, 700, 600, 700, buffer, 300, backendFile.length()),
        new ReadRequest(800, 900, 800, 900, buffer, 400, backendFile.length()),
        new ReadRequest(1000, 1100, 1000, 1100, buffer, 500, backendFile.length()),
        new ReadRequest(1200, 1300, 1200, 1300, buffer, 600, backendFile.length()),
        new ReadRequest(1400, 1500, 1400, 1500, buffer, 700, backendFile.length()),
        new ReadRequest(1600, 1700, 1600, 1700, buffer, 800, backendFile.length()),
        new ReadRequest(1800, 1900, 1800, 1900, buffer, 900, backendFile.length())
    };
  }
}
