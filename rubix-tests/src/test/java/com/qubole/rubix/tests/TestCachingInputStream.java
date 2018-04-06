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
package com.qubole.rubix.tests;

import com.qubole.rubix.bookkeeper.BookKeeperServer;
import com.qubole.rubix.bookkeeper.LocalDataTransferServer;
import com.qubole.rubix.core.CachingFileSystemStats;
import com.qubole.rubix.core.CachingInputStream;
import com.qubole.rubix.core.LocalFSInputStream;
import com.qubole.rubix.core.utils.DataGen;
import com.qubole.rubix.core.utils.DeleteFileVisitor;
import com.qubole.rubix.spi.BookKeeperFactory;
import com.qubole.rubix.spi.CacheConfig;
import com.qubole.rubix.spi.ClusterType;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;

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
    log.info("Deleting files in " + testDirectory);
    Files.walkFileTree(Paths.get(testDirectory), new DeleteFileVisitor());
    Files.deleteIfExists(Paths.get(testDirectory));
  }

  @BeforeMethod
  public void setup() throws IOException, InterruptedException, URISyntaxException
  {
    final Configuration conf = new Configuration();

    conf.setBoolean(CacheConfig.DATA_CACHE_STRICT_MODE, true);
    conf.setInt(CacheConfig.dataCacheBookkeeperPortConf, 3456);
    conf.setInt(CacheConfig.localServerPortConf, 2222);
    conf.set(CacheConfig.dataCacheDirprefixesConf, testDirectoryPrefix + "dir");
    conf.setBoolean(CacheConfig.parallelWarmupEnable, false);
    Thread server = new Thread()
    {
      public void run()
      {
        LocalDataTransferServer.startServer(conf);
      }
    };
    server.start();
    Thread thread = new Thread()
    {
      public void run()
      {
        BookKeeperServer.startServer(conf);
      }
    };
    thread.start();

    DataGen.populateFile(backendFileName);

    while (!BookKeeperServer.isServerUp()) {
      Thread.sleep(200);
      log.info("Waiting for BookKeeper Server to come up");
    }
    createCachingStream(conf);
    log.info("BackendPath: " + backendPath);
  }

  public void createCachingStream(Configuration conf)
      throws InterruptedException, IOException, URISyntaxException
  {
    conf.setBoolean(CacheConfig.DATA_CACHE_STRICT_MODE, true);
    conf.setInt(CacheConfig.dataCacheBookkeeperPortConf, 3456);

    File file = new File(backendFileName);

    LocalFSInputStream localFSInputStream = new LocalFSInputStream(backendFileName);
    FSDataInputStream fsDataInputStream = new FSDataInputStream(localFSInputStream);
    conf.setInt(CacheConfig.blockSizeConf, blockSize);
    // This should be after server comes up else client could not be created
    inputStream = new CachingInputStream(fsDataInputStream, conf, backendPath, file.length(),
        file.lastModified(), new CachingFileSystemStats(), ClusterType.TEST_CLUSTER_MANAGER,
        new BookKeeperFactory(), FileSystem.get(new URI(backendFileName), conf),
        CacheConfig.getBlockSize(conf), null);
  }

  @AfterMethod
  public void cleanup()
  {
    BookKeeperServer.stopServer();
    LocalDataTransferServer.stopServer();
    Configuration conf = new Configuration();
    conf.set(CacheConfig.dataCacheDirprefixesConf, testDirectoryPrefix + "dir");
    inputStream.close();
    File file = new File(backendFileName);
    file.delete();

    File mdFile = new File(CacheConfig.getMDFile(backendPath.toString(), conf));
    mdFile.delete();

    File localFile = new File(CacheConfig.getLocalPath(backendPath.toString(), conf));
    localFile.delete();
  }

  @Test
  public void testCaching() throws IOException, InterruptedException
  {
    // 1. Seek and read
    testCachingHelper();

    // 2. Delete backend file
    File file = new File(backendFileName);
    file.delete();

    // 3. Read the same data to ensure that data read from cache correctly
    testCachingHelper();
  }

  private void testCachingHelper()
      throws IOException
  {
    inputStream.seek(100);
    byte[] buffer = new byte[1000];
    int readSize = inputStream.read(buffer, 0, 1000);
    String output = new String(buffer, Charset.defaultCharset());
    String expectedOutput = DataGen.generateContent().substring(100, 1100);
    assertions(readSize, 1000, buffer, expectedOutput);
  }

  @Test
  public void testChunkCachingAndEviction()
      throws IOException, InterruptedException, URISyntaxException
  {
    // 1. Seek and read some data
    testCachingHelper();

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
    writeZeros(backendFileName, 100, 1100);
    writeZeros(backendFileName, 1550, 1750);

    // 5. Read from [0, 1750) and ensure the old data is returned, this verifies that reading in chunks, some from cache and some from backend works as expected
    Thread.sleep(3000);
    buffer = new byte[1750];
    inputStream.seek(0);
    readSize = inputStream.read(buffer, 0, 1750);
    expectedOutput = DataGen.generateContent().substring(0, 1750);
    assertions(readSize, 1750, buffer, expectedOutput);

    //6. Close existing stream and start a new one to get the new lastModifiedDate of backend file
    inputStream.close();
    Configuration conf = new Configuration();
    conf.set(CacheConfig.dataCacheDirprefixesConf, testDirectoryPrefix + "dir");
    createCachingStream(conf);
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

  private void writeZeros(String filename, int start, int end) throws IOException
  {
    File file = new File(filename);
    RandomAccessFile raf = new RandomAccessFile(file, "rw");
    raf.seek(start);
    String s = "0";
    StandardCharsets.UTF_8.encode(s);
    for (int i = 0; i < (end - start); i++) {
      raf.writeBytes(s);
    }
    raf.close();
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
