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
package com.qubole.rubix.tests;

import com.qubole.rubix.core.utils.DataGen;
import com.qubole.rubix.core.utils.DeleteFileVisitor;
import com.qubole.rubix.spi.BookKeeperFactory;
import com.qubole.rubix.spi.CacheConfig;
import com.qubole.rubix.spi.CacheUtil;
import com.qubole.rubix.spi.RetryingBookkeeperClient;
import com.qubole.rubix.spi.thrift.BlockLocation;
import com.qubole.rubix.spi.thrift.CacheStatusRequest;
import com.qubole.rubix.spi.thrift.Location;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.thrift.shaded.TException;
import org.apache.thrift.shaded.transport.TTransportException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import static org.testng.Assert.assertTrue;

/**
 * Created by kvankayala on 10 Jul 2018.
 * TestThriftServerJVM Class created to test separate JVMs for bookKeeper and LDS servers
 */
public class TestThriftServerJVM extends Configured
{
  private static final String testDirectoryPrefix = System.getProperty("java.io.tmpdir") + "/TestThriftServerJVM/";
  private static String backendFileName = testDirectoryPrefix + "backendDataFile";
  private static Path backendPath = new Path("file:///" + backendFileName.substring(1));

  private static final String testDirectory = testDirectoryPrefix + "dir0";
  private static final Log log = LogFactory.getLog(TestThriftServerJVM.class.getName());

  private static final String jarsPath = "/usr/lib/hadoop2/share/hadoop/tools/lib/";
  private static final String hadoopDirectory = "/usr/lib/hadoop2/bin/hadoop";
  private static final String bookKeeperClass = "com.qubole.rubix.bookkeeper.BookKeeperServer";
  private static final String localDataTransferServerClass = "com.qubole.rubix.bookkeeper.LocalDataTransferServer";
  private static final String setDataBlockSize = "-Dhadoop.cache.data.block-size=200";
  private static final String setCacheMaxDisks = "-Dhadoop.cache.data.max.disks=1";
  private static final String setCacheDirectory = "-Dhadoop.cache.data.dirprefix.list=" + testDirectoryPrefix + "dir";
  private static final String setmasterbookkeeper = "-Drubix.cluster.on-master=true";

  public BookKeeperFactory bookKeeperFactory = new BookKeeperFactory();

  static Process bookKeeperJvm;
  static Process localDataTransferJvm;

  private static Configuration conf = new Configuration();

  @BeforeClass
  public static void setupClass() throws IOException, InterruptedException
  {
    /*
     * Dynamically, retrieving bookkeeper jar from /usr/lib/hadoop2/share/hadoop/tools/lib/ folder
     * */
    log.info("Test Directory : " + testDirectory);
    Files.createDirectories(Paths.get(testDirectoryPrefix + "dir0" + CacheConfig.getCacheDataDirSuffix(conf)));
    File folder = new File(jarsPath);
    File[] listOfFiles = folder.listFiles();
    String bookKeeperJarPath = null;
    for (int i = 0; i < listOfFiles.length; i++) {
      if (listOfFiles[i].isFile() && listOfFiles[i].toString().contains("bookkeeper")) {
        bookKeeperJarPath = listOfFiles[i].toString();
      }
    }
    log.debug(" Located Bookkeeper Jar is : " + bookKeeperJarPath);
    /*
     * Spinning up the separate JVMs for bookKeeper and Local Data Transfer Servers
     * */
    String[] bookKeeperStartCmd = {hadoopDirectory, "jar", bookKeeperJarPath, bookKeeperClass, setDataBlockSize, setCacheMaxDisks, setCacheDirectory, setmasterbookkeeper};
    String[] localDataTransferStartCmd = {hadoopDirectory, "jar", bookKeeperJarPath, localDataTransferServerClass, setDataBlockSize, setCacheMaxDisks, setCacheDirectory, setmasterbookkeeper};

    ProcessBuilder pJVMBuilder = new ProcessBuilder();
    pJVMBuilder.redirectErrorStream(true);
    pJVMBuilder.inheritIO();
    pJVMBuilder.command(bookKeeperStartCmd);
    bookKeeperJvm = pJVMBuilder.start();
    pJVMBuilder.command(localDataTransferStartCmd);
    localDataTransferJvm = pJVMBuilder.start();
    Thread.sleep(3000);
  }

  @AfterClass
  public static void tearDownClass() throws IOException
  {
    /* ****
      Destroying bookKeeper and localDataTransfer JVMs
    **** */
    bookKeeperJvm.destroy();
    localDataTransferJvm.destroy();

    log.info("Deleting files in " + testDirectory);
    Files.walkFileTree(Paths.get(testDirectory), new DeleteFileVisitor());
    Files.deleteIfExists(Paths.get(testDirectory));
  }

  @BeforeMethod
  public static void setup() throws IOException, InterruptedException, URISyntaxException
  {
    DataGen.populateFile(backendFileName);

    CacheConfig.setIsStrictMode(conf, true);
    CacheConfig.setCacheDataDirPrefix(conf, testDirectoryPrefix + "dir");
    CacheConfig.setMaxDisks(conf, 1);
    CacheConfig.setBlockSize(conf, 200);
  }

  @AfterMethod
  public static void cleanup()
  {
    File file = new File(backendFileName);
    file.delete();

    File mdFile = new File(CacheUtil.getMetadataFilePath(backendPath.toString(), conf));
    mdFile.delete();

    File localFile = new File(CacheUtil.getLocalPath(backendPath.toString(), conf));
    localFile.delete();
  }

  @Test(enabled = true)
  public void testBookKeeperCaching() throws IOException, InterruptedException, TTransportException, TException
  {
    String host = "localhost";
    File file = new File(backendFileName);
    List<BlockLocation> result;
    long lastBlock = file.length() / CacheConfig.getBlockSize(conf);
    long readSize = file.length();

    RetryingBookkeeperClient client;
    client = bookKeeperFactory.createBookKeeperClient(host, conf);

    CacheStatusRequest request = new CacheStatusRequest(backendPath.toString(), file.length(), file.lastModified(), 0, lastBlock, 3);
    result = client.getCacheStatus(request);

    assertTrue(result.get(0).getLocation() == Location.LOCAL, "File already cached, before readData call");
    log.info(" Value of Result : " + result);
    log.info("Downloading file from path : " + file.toString());
    boolean dataDownloaded = client.readData(backendPath.toString(), 0, (int) readSize, file.length(), file.lastModified(), 3);
    assertTrue(dataDownloaded == true, "readData() function call failed. File not downloaded properly");

    result = client.getCacheStatus(request);
    assertTrue(result.get(0).getLocation() == Location.CACHED, "File not cached properly");
    log.info(" Value of Result : " + result);
  }

  @Test(enabled = true, expectedExceptions = org.apache.thrift.shaded.transport.TTransportException.class)
  public void testCreateBookKeeperClient_OnNotRunningPort() throws IOException, InterruptedException, Exception
  {
    //1234 is a random port to test if TTransportException is being thrown or not
    CacheConfig.setServerPort(conf, 1234);
    String host = "localhost";
    bookKeeperFactory.createBookKeeperClient(host, conf);
  }
}
