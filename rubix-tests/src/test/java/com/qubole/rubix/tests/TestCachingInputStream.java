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

import com.codahale.metrics.MetricRegistry;
import com.qubole.rubix.bookkeeper.BookKeeperServer;
import com.qubole.rubix.bookkeeper.LocalDataTransferServer;
import com.qubole.rubix.common.utils.DataGen;
import com.qubole.rubix.common.utils.DeleteFileVisitor;
import com.qubole.rubix.core.CachingFileSystemStats;
import com.qubole.rubix.core.CachingInputStream;
import com.qubole.rubix.spi.BookKeeperFactory;
import com.qubole.rubix.spi.CacheConfig;
import com.qubole.rubix.spi.CacheUtil;
import com.qubole.rubix.spi.ClusterType;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;

import static com.qubole.rubix.spi.CacheUtil.UNKONWN_GENERATION_NUMBER;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * Created by stagra on 25/1/16.
 */
public class TestCachingInputStream
{
  int blockSize = 100;
  private static final String testDirectoryPrefix = System.getProperty("java.io.tmpdir") + "/TestCachingInputStream/";
  String backendFileName = testDirectoryPrefix + "backendFile";
  Path backendPath = new Path("file:///" + backendFileName.substring(1));

  CachingInputStream inputStream;

  private static final String testDirectory = testDirectoryPrefix + "dir0";
  private static Configuration conf;
  private BookKeeperServer bookKeeperServer;

  private static final Log log = LogFactory.getLog(TestCachingInputStream.class.getName());

  @BeforeClass
  public static void setupClass() throws IOException
  {
    log.info(testDirectory);
    Files.createDirectories(Paths.get(testDirectory));
  }

  @AfterClass
  public static void tearDownClass() throws IOException
  {
    BookKeeperFactory.resetConnectionPool();
    log.info("Deleting files in " + testDirectory);
    Files.walkFileTree(Paths.get(testDirectory), new DeleteFileVisitor());
    Files.deleteIfExists(Paths.get(testDirectory));
  }

  @BeforeMethod
  public void setup() throws IOException, InterruptedException, URISyntaxException
  {
    BookKeeperFactory.resetConnectionPool();
    conf = new Configuration();

    CacheConfig.setOnMaster(conf, true);
    CacheConfig.setIsStrictMode(conf, true);
    CacheConfig.setBookKeeperServerPort(conf, 3456);
    CacheConfig.setDataTransferServerPort(conf, 2222);
    CacheConfig.setCacheDataDirPrefix(conf, testDirectoryPrefix + "dir");
    CacheConfig.setMaxDisks(conf, 1);
    CacheConfig.setIsParallelWarmupEnabled(conf, false);
    CacheConfig.setOnMaster(conf, true);
    CacheConfig.setBlockSize(conf, blockSize);
    CacheConfig.setFileStalenessCheck(conf, true);

    Thread server = new Thread()
    {
      public void run()
      {
        LocalDataTransferServer.startServer(conf, new MetricRegistry());
      }
    };
    server.start();
    bookKeeperServer = new BookKeeperServer();
    Thread thread = new Thread()
    {
      public void run()
      {
        bookKeeperServer.startServer(conf, new MetricRegistry());
      }
    };
    thread.start();

    DataGen.populateFile(backendFileName);

    while (!bookKeeperServer.isServerUp()) {
      Thread.sleep(200);
      log.info("Waiting for BookKeeper Server to come up");
    }
    inputStream = createCachingStream(conf);
    log.info("BackendPath: " + backendPath);
  }

  private CachingInputStream createCachingStream(Configuration conf)
      throws IOException
  {
    FileSystem localFileSystem = new RawLocalFileSystem();
    Path backendFilePath = new Path(backendFileName);
    localFileSystem.initialize(backendFilePath.toUri(), new Configuration());
    CacheConfig.setBlockSize(conf, blockSize);

    // This should be after server comes up else client could not be created
    return new CachingInputStream(backendPath, conf,
        new CachingFileSystemStats(), ClusterType.TEST_CLUSTER_MANAGER,
        new BookKeeperFactory(), localFileSystem,
        CacheConfig.getBlockSize(conf), null);
  }

  @AfterMethod
  public void cleanup()
  {
    if (bookKeeperServer != null) {
      bookKeeperServer.stopServer();
    }
    LocalDataTransferServer.stopServer();

    inputStream.close();
    File file = new File(backendFileName);
    file.delete();

    File mdFile = new File(CacheUtil.getMetadataFilePath(backendPath.toString(), conf, UNKONWN_GENERATION_NUMBER + 1));
    mdFile.delete();

    File localFile = new File(CacheUtil.getLocalPath(backendPath.toString(), conf, UNKONWN_GENERATION_NUMBER + 1));
    localFile.delete();

    conf.clear();
  }

  @Test
  public void testCaching() throws IOException, InterruptedException
  {
    // 1. Seek and read
    testCachingHelper(false);

    // 2. Delete backend file
    File file = new File(backendFileName);
    file.delete();

    // 3. Read the same data to ensure that data read from cache correctly
    testCachingHelper(false);
  }

  @Test
  public void testCachingPositionedRead() throws IOException, InterruptedException
  {
    // 1. Positioned read
    testCachingHelper(true);

    // 2. Delete backend file
    File file = new File(backendFileName);
    file.delete();

    // 3. Read the same data to ensure that data read from cache correctly
    testCachingHelper(true);
  }

  @Test
  public void testStaleCachedRead() throws IOException, InterruptedException
  {
    Configuration staleConf = new Configuration(conf);
    CacheConfig.setFileStalenessCheck(staleConf, false);
    try (CachingInputStream inputStream = createCachingStream(staleConf)) {
      byte[] buffer = new byte[1000];
      int readSize = inputStream.read(200, buffer, 0, 1000);
      assertions(readSize, 1000, buffer, DataGen.generateContent().substring(200, 1200));

      Thread.sleep(3000); // sleep to give server chance to update cache status

      //Rewrite the file with half the data
      DataGen.populateFile(backendFileName, 2);

      // Without staleness check, we should read old data
      readSize = inputStream.read(200, buffer, 0, 1000);
      assertions(readSize, 1000, buffer, DataGen.generateContent().substring(200, 1200));
    }
  }

  private void testCachingHelper(boolean positionedRead)
      throws IOException
  {
    byte[] buffer = new byte[1000];
    int readSize;
    if (positionedRead) {
      readSize = inputStream.read(100, buffer, 0, 1000);
      assertEquals(inputStream.getPos(), 0);
    }
    else {
      inputStream.seek(100);
      readSize = inputStream.read(buffer, 0, 1000);
      assertEquals(inputStream.getPos(), 100 + readSize);
    }
    String expectedOutput = DataGen.generateContent().substring(100, 1100);
    assertions(readSize, 1000, buffer, expectedOutput);
  }

  @Test
  public void testChunkCachingAndEviction()
      throws IOException, InterruptedException
  {
    // 1. Seek and read some data
    testCachingHelper(false);

    // 2. Skip more than a block worth of data
    Thread.sleep(3000); // sleep to give server chance to update cache status
    inputStream.seek(1550);

    // 3. Read some more data from 1550
    Thread.sleep(3000);
    byte[] buffer = new byte[200];
    int readSize = inputStream.read(buffer, 0, 200);
    String expectedOutput = DataGen.generateContent().substring(1550, 1750);
    assertions(readSize, 200, buffer, expectedOutput);

    // 4. Replace chunks already read from backend file with zeros
    DataGen.writeZerosInFile(backendFileName, 100, 1100);
    DataGen.writeZerosInFile(backendFileName, 1550, 1750);

    // 5. Read from [0, 1750) and ensure the old data is returned, this verifies that reading in chunks, some from cache and some from backend works as expected
    Thread.sleep(3000);
    buffer = new byte[1750];
    inputStream.seek(0);
    readSize = inputStream.read(buffer, 0, 1750);
    expectedOutput = DataGen.generateContent().substring(0, 1750);
    assertions(readSize, 1750, buffer, expectedOutput);

    //6. Close existing stream and start a new one to get the new lastModifiedDate of backend file
    inputStream.close();

    inputStream = createCachingStream(conf);
    log.info("New stream started");

    //7. Read the data again and verify that correct, updated data is being read from the backend file and that the previous cache entry is evicted.
    buffer = new byte[1000];
    inputStream.seek(100);
    readSize = inputStream.read(buffer, 0, 1000);

    StringBuilder stringBuilder = new StringBuilder();
    for (int j = 0; j < 1000; j++) {
      stringBuilder.append(0);
    }
    expectedOutput = stringBuilder.toString();

    assertions(readSize, 1000, buffer, expectedOutput);
  }

  @Test
  public void testEOF() throws IOException
  {
    inputStream.seek(2500);
    byte[] buffer = new byte[200];
    int readSize = inputStream.read(buffer, 0, 200);

    String expectedOutput = DataGen.generateContent().substring(2500);
    assertions(readSize, 100, Arrays.copyOf(buffer, readSize), expectedOutput);

    readSize = inputStream.read(buffer, 100, 100);
    assertTrue(readSize == -1, "Did not get EOF");
  }

  private void assertions(int readSize, int expectedReadSize, byte[] outputBuffer, String expectedOutput)
  {
    assertTrue(readSize == expectedReadSize, "Wrong amount of data read " + readSize + " was expecting " + expectedReadSize);
    String output = new String(outputBuffer, Charset.defaultCharset());
    assertTrue(expectedOutput.equals(output), "Wrong data read, expected\n" + expectedOutput + "\nBut got\n" + output);
  }
}
