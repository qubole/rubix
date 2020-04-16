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

package com.qubole.rubix.tests;

/**
 * Created by sakshia on 25/11/16.
 */

import com.codahale.metrics.MetricRegistry;
import com.qubole.rubix.bookkeeper.BookKeeperServer;
import com.qubole.rubix.bookkeeper.LocalDataTransferServer;
import com.qubole.rubix.common.utils.DataGen;
import com.qubole.rubix.common.utils.DeleteFileVisitor;
import com.qubole.rubix.core.MockCachingFileSystem;
import com.qubole.rubix.core.NonLocalReadRequestChain;
import com.qubole.rubix.core.ReadRequest;
import com.qubole.rubix.spi.BookKeeperFactory;
import com.qubole.rubix.spi.CacheConfig;
import com.qubole.rubix.spi.CacheUtil;
import com.qubole.rubix.spi.ClusterType;
import com.qubole.rubix.spi.RetryingPooledBookkeeperClient;
import com.qubole.rubix.spi.thrift.BlockLocation;
import com.qubole.rubix.spi.thrift.CacheStatusRequest;
import com.qubole.rubix.spi.thrift.Location;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.thrift.shaded.TException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestNonLocalReadRequestChain
{
  int blockSize = 100;
  private static final String testDirectoryPrefix = System.getProperty("java.io.tmpdir") + "/TestNonLocalReadRequestChain/";
  String backendFileName = testDirectoryPrefix + "backendFile";
  Path backendPath = new Path("file:///" + backendFileName.substring(1));
  File backendFile;
  final Configuration conf = new Configuration();
  Thread localDataTransferServer;

  private static final String testDirectory = testDirectoryPrefix + "dir0";
  private BookKeeperServer bookKeeperServer;

  NonLocalReadRequestChain nonLocalReadRequestChain;
  private static final Log log = LogFactory.getLog(TestNonLocalReadRequestChain.class);

  @BeforeClass
  public static void setupClass() throws IOException
  {
    log.info(testDirectory);
    Files.createDirectories(Paths.get(testDirectory));
  }

  @AfterClass
  public static void tearDownClass() throws IOException
  {
    log.info("Deleting files in " + testDirectory);
    Files.walkFileTree(Paths.get(testDirectory), new DeleteFileVisitor());
    Files.deleteIfExists(Paths.get(testDirectory));
  }

  @BeforeMethod
  public void setup()
      throws Exception
  {
    CacheConfig.setOnMaster(conf, true);
    CacheConfig.setIsStrictMode(conf, true);
    CacheConfig.setBookKeeperServerPort(conf, 3456);
    CacheConfig.setDataTransferServerPort(conf, 2222);
    CacheConfig.setBlockSize(conf, blockSize);
    CacheConfig.setCacheDataDirPrefix(conf, testDirectoryPrefix + "dir");
    CacheConfig.setMaxDisks(conf, 1);
    CacheConfig.setIsParallelWarmupEnabled(conf, false);
    CacheConfig.setOnMaster(conf, true);

    localDataTransferServer = new Thread()
    {
      public void run()
      {
        LocalDataTransferServer.startServer(conf, new MetricRegistry());
      }
    };
    bookKeeperServer = new BookKeeperServer();
    Thread thread = new Thread()
    {
      public void run()
      {
        bookKeeperServer.startServer(conf, new MetricRegistry());
      }
    };
    thread.start();

    while (!bookKeeperServer.isServerUp()) {
      Thread.sleep(200);
      log.info("Waiting for BookKeeper Server to come up");
    }

    // Populate File
    DataGen.populateFile(backendFileName);
    backendFile = new File(backendFileName);

    //set class for filepath beginning with testfile
    conf.setClass("fs.testfile.impl", MockCachingFileSystem.class, FileSystem.class);
    nonLocalReadRequestChain = createNewNonLocalReadRequest();
    BookKeeperFactory.resetConnectionPool();
  }

  private NonLocalReadRequestChain createNewNonLocalReadRequest() throws IOException
  {
    MockCachingFileSystem fs = new MockCachingFileSystem();
    fs.initialize(backendPath.toUri(), conf);
    NonLocalReadRequestChain requestChain = new NonLocalReadRequestChain("localhost", backendFile.length(),
        backendFile.lastModified(), conf, fs, backendPath.toString(),
        ClusterType.TEST_CLUSTER_MANAGER.ordinal(), false, null);

    return requestChain;
  }

  @Test
  private void testRemoteRead()
      throws Exception
  {
    localDataTransferServer.start();
    while (!LocalDataTransferServer.isServerUp()) {
      Thread.sleep(200);
      log.info("Waiting for Local Data Transfer Server to come up");
    }
    nonLocalReadRequestChain.strictMode = true;
    test(nonLocalReadRequestChain);
  }

  @Test
  private void testDirectRead()
      throws Exception
  {
    nonLocalReadRequestChain.strictMode = false;
    test(nonLocalReadRequestChain);
  }

  @Test
  private void testDirectRead2()
      throws Exception
  {
    localDataTransferServer.start();
    if (bookKeeperServer != null) {
      bookKeeperServer.stopServer();
    }
    while (!LocalDataTransferServer.isServerUp()) {
      Thread.sleep(200);
      log.info("Waiting for Local Data Transfer Server to come up");
    }
    nonLocalReadRequestChain.strictMode = false;
    test(nonLocalReadRequestChain);
  }

  public void test(NonLocalReadRequestChain requestChain) throws Exception
  {
    Logger.getRootLogger().setLevel(Level.INFO);
    byte[] buffer = new byte[350];

    ReadRequest[] readRequests = {
        new ReadRequest(0, 100, 50, 100, buffer, 0, backendFile.length()),
        new ReadRequest(200, 300, 200, 300, buffer, 50, backendFile.length()),
        new ReadRequest(400, 500, 400, 500, buffer, 150, backendFile.length()),
        new ReadRequest(600, 700, 600, 700, buffer, 250, backendFile.length()),
    };

    //1. send non-local readrequest
    for (ReadRequest rr : readRequests) {
      requestChain.addReadRequest(rr);
    }

    requestChain.lock();

    // 2. Execute and verify that buffer has right data
    int readSize = requestChain.call();

    assertTrue(readSize == 350, "Wrong amount of data read " + readSize + " was expecting " + 350);
    String output = new String(buffer, Charset.defaultCharset());
    String expectedOutput = DataGen.getExpectedOutput(1000).substring(50, 400);
    assertTrue(expectedOutput.equals(output), "Wrong data read, expected\n" + expectedOutput + "\nBut got\n" + output);
  }

  @Test
  public void testRemoteRead_WhenAllBlocksAreCached() throws Exception
  {
    localDataTransferServer.start();
    prepopulateCache(conf, 700);

    CacheConfig.setCleanupFilesDuringStart(conf, false);
    CacheConfig.setIsParallelWarmupEnabled(conf, true);

    stopAllServers();
    startAllServers();

    NonLocalReadRequestChain request = createNewNonLocalReadRequest();
    test(request);

    assertTrue(request.getStats().getNonLocalDataRead() == 350, "Total non-local data read didn't match");
    assertTrue(request.getStats().getRequestedRead() == 0, "Total direct data read didn't match");
  }

  @Test(expectedExceptions = Exception.class)
  public void testRemoteRead_WhenSomeBlocksAreCached_WithStrictMode_ParallelWarmup() throws Exception
  {
    localDataTransferServer.start();

    prepopulateCache(conf, 500);

    CacheConfig.setCleanupFilesDuringStart(conf, false);
    CacheConfig.setIsParallelWarmupEnabled(conf, true);

    stopAllServers();
    startAllServers();

    NonLocalReadRequestChain request = createNewNonLocalReadRequest();
    request.strictMode = true;
    test(request);
  }

  @Test
  public void testRemoteRead_WhenSomeBlocksAreCached_WithoutStrictMode_ParallelWarmup() throws Exception
  {
    localDataTransferServer.start();

    prepopulateCache(conf, 500);

    CacheConfig.setCleanupFilesDuringStart(conf, false);
    CacheConfig.setIsParallelWarmupEnabled(conf, true);

    stopAllServers();
    startAllServers();

    NonLocalReadRequestChain request = createNewNonLocalReadRequest();
    request.strictMode = false;
    test(request);

    assertTrue(request.getStats().getNonLocalDataRead() == 250, "Total non-local data read didn't match");
    assertTrue(request.getStats().getRequestedRead() == 100, "Total direct data read didn't match");
  }

  @Test
  public void testRemoteRead_WhenSomeBlocksAreCached_WithStrictMode_NoParallelWarmup() throws Exception
  {
    localDataTransferServer.start();

    prepopulateCache(conf, 500);

    CacheConfig.setCleanupFilesDuringStart(conf, false);
    CacheConfig.setIsParallelWarmupEnabled(conf, false);

    stopAllServers();
    startAllServers();

    NonLocalReadRequestChain request = createNewNonLocalReadRequest();
    request.strictMode = true;

    test(request);

    assertTrue(request.getStats().getNonLocalDataRead() == 350, "Total non-local data read didn't match");
    assertTrue(request.getStats().getRequestedRead() == 0, "Total direct data read didn't match");
  }

  private void startAllServers() throws InterruptedException
  {
    localDataTransferServer = new Thread()
    {
      public void run()
      {
        LocalDataTransferServer.startServer(conf, new MetricRegistry());
      }
    };
    localDataTransferServer.start();

    bookKeeperServer = new BookKeeperServer();
    Thread thread = new Thread()
    {
      public void run()
      {
        bookKeeperServer.startServer(conf, new MetricRegistry());
      }
    };
    thread.start();

    while (!bookKeeperServer.isServerUp()) {
      Thread.sleep(200);
    }

    while (!LocalDataTransferServer.isServerUp()) {
      Thread.sleep(200);
    }
  }

  private void stopAllServers()
  {
    if (bookKeeperServer != null) {
      bookKeeperServer.stopServer();
    }
    LocalDataTransferServer.stopServer();
  }

  private void prepopulateCache(Configuration conf, int length) throws TException
  {
    int endBlock = length / CacheConfig.getBlockSize(conf);
    BookKeeperFactory factory = new BookKeeperFactory();

    RetryingPooledBookkeeperClient client = factory.createBookKeeperClient("localhost", conf);
    client.readData(backendPath.toString(), 0, length, backendFile.length(),
        backendFile.lastModified(), ClusterType.TEST_CLUSTER_MANAGER.ordinal());

    CacheStatusRequest request = new CacheStatusRequest(backendPath.toString(), backendFile.length(), backendFile.lastModified(),
        0, endBlock, ClusterType.TEST_CLUSTER_MANAGER.ordinal());
    List<BlockLocation> blockLocations = client.getCacheStatus(request);

    for (BlockLocation location : blockLocations) {
      if (location.getLocation() != Location.CACHED) {
        fail();
      }
    }
  }

  @AfterMethod
  public void cleanup()
      throws IOException
  {
    stopAllServers();

    File mdFile = new File(CacheUtil.getMetadataFilePath(backendPath.toString(), conf));
    mdFile.delete();

    File localFile = new File(CacheUtil.getLocalPath(backendPath.toString(), conf));
    localFile.delete();
    backendFile.delete();

    conf.clear();
  }
}
