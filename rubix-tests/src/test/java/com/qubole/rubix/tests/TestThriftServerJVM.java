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

import com.qubole.rubix.bookkeeper.LocalDataTransferServer;
import com.qubole.rubix.core.utils.DataGen;
import com.qubole.rubix.core.utils.DeleteFileVisitor;
import com.qubole.rubix.spi.BlockLocation;
import com.qubole.rubix.spi.BookKeeperFactory;
import com.qubole.rubix.spi.CacheConfig;
import com.qubole.rubix.spi.RetryingBookkeeperClient;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.DirectBufferPool;
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
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

import static org.testng.Assert.assertTrue;

public class TestThriftServerJVM extends Configured
{
    private static final String testDirectoryPrefix = System.getProperty("java.io.tmpdir") + "/TestThriftServerJVM/";
    //String backendFileName = "/Users/kvankayala/src/rubix/rubix-tests/output";//testDirectoryPrefix + "backendDataFile";
    String backendFileName = testDirectoryPrefix + "backendDataFile";
    Path backendPath = new Path("file:///" + backendFileName.substring(1));

    private static final String testDirectory = testDirectoryPrefix + "dir0";
    private static final Log log = LogFactory.getLog(TestThriftServerJVM.class.getName());

    private static final String jarsPath = "/usr/lib/hadoop2/share/hadoop/tools/lib/";  // Use HADOOP_PREFIX for jarsPath
    private static final String hadoopDirectory = "/usr/lib/hadoop2/bin/hadoop";
    private static final String bookKeeperClass = "com.qubole.rubix.bookkeeper.BookKeeperServer";
    private static final String localDataTransferServerClass = "com.qubole.rubix.bookkeeper.BookKeeperServer";
    private static final String setDataBlockSize = "-Dhadoop.cache.data.block-size=200";
    private static final String setCacheMaxDisks = "-Dhadoop.cache.data.max.disks=2";

    public BookKeeperFactory bookKeeperFactory = new BookKeeperFactory();
    public RetryingBookkeeperClient client;

    static Process bookKeeperJvm;
    static Process localDataTransferJvm;

    @BeforeClass
    public static void setupClass() throws IOException, InterruptedException
    {
        /*
         * Dynamically, retrieving bookkeeper jar from /usr/lib/hadoop2/share/hadoop/tools/lib/ folder
         * */
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
         * Spinning up the separate JVMs for bookKeeper and Local Data Transfer
         * */
        String[] bookKeeperStartCmd = {hadoopDirectory, "jar", bookKeeperJarPath, bookKeeperClass, setDataBlockSize, setCacheMaxDisks};
        String[] localDataTransferStartCmd = {hadoopDirectory, "jar", bookKeeperJarPath, localDataTransferServerClass, setDataBlockSize, setCacheMaxDisks};

        ProcessBuilder pJVMBuilder = new ProcessBuilder();
        pJVMBuilder.redirectErrorStream(true);
        pJVMBuilder.inheritIO();
        pJVMBuilder.command(bookKeeperStartCmd);
        bookKeeperJvm = pJVMBuilder.start();
        pJVMBuilder.command(localDataTransferStartCmd);
        localDataTransferJvm = pJVMBuilder.start();

        log.info("Test Directory : " + testDirectory);
        Files.createDirectories(Paths.get(testDirectory));
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
    public void setup() throws IOException, InterruptedException, URISyntaxException
    {
        DataGen.populateFile(backendFileName);
        log.info("BackendPath: " + backendPath);
    }

    @AfterMethod
    public void cleanup()
    {
        File file = new File(backendFileName);
        file.delete();
    }

    @Test
    public void testJVMCommunication() throws IOException, InterruptedException
    {
        log.info("Value of Path " + this.backendFileName);
        log.debug(" backendPath to string : " + this.backendPath.toString());
        Configuration conf = new Configuration();
        //conf = this.getConf();
        CacheConfig.setIsStrictMode(conf, true);
        CacheConfig.setCacheDataDirPrefix(conf, testDirectoryPrefix + "dir");
        CacheConfig.setMaxDisks(conf, 2);
        CacheConfig.setIsParallelWarmupEnabled(conf, false);
        CacheConfig.setBlockSize(conf, 200);
        //CacheConfig.get

        String host = "localhost";
        boolean dataDownloaded;
        File file = new File(backendFileName);
        List<BlockLocation> result;

        try {
            client = bookKeeperFactory.createBookKeeperClient(host, conf);

            /*File folder = new File(testDirectoryPrefix);
            File[] listOfFiles = folder.listFiles();
            for (int i = 0; i < listOfFiles.length; i++) {
                if (listOfFiles[i].isFile() && listOfFiles[i].toString().contains("backendDataFile")) {
                    log.info( " backendDataFile file is present ");
                }
            }*/


            int lastBlock = 4;//file.length()/200 +1;
            log.debug("The File name in remoteFile variable is : " + file.toString() + " file size " + file.length() + " last modified : " + file.lastModified());
            result = client.getCacheStatus(file.toString(), file.length(), file.lastModified(), 0, lastBlock, 3);
            //assertTrue(result.get(0).remoteLocation == "LOCAL", "File already cached, before readData call");

            log.info(" Value of Result : " + result );
            /*log.info("After get status call : Size of result : " + result.size());
            log.info("Remote Location in result : " + result.get(0).remoteLocation);
            log.info("Location in result : " + result.get(0).location);
            log.info("Values in result : " + result.get(0).toString());*/
            /*assertTrue(result.size() == 1, "Wrong Size of return value getCacheStatus function " + statsMap.size() + " was expecting 1 ");*/

            /*Map<String, Double> statsMap = client.getCacheStats();
            assertTrue(statsMap.size() == 5, "Wrong Size of Stats " + statsMap.size() + " was expecting 5 ");
            Iterator<Map.Entry<String, Double>> it = statsMap.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<String, Double> pair = it.next();
                log.debug("Key is : " + pair.getKey() + " /// Value is : " + pair.getValue());
            }*/

            int readSize = 1000;
            log.info("Downloading data from path : " + file.toString());
            dataDownloaded = client.readData(file.toString(), 0, readSize, file.length(), file.lastModified(), 3);
            if (!dataDownloaded) {
                log.info("Failed to read Data from the location");
            }

            log.debug("The File name in remoteFile variable is : " + file.toString() + " file size " + file.length() + " last modified : " + file.lastModified());
            result = client.getCacheStatus(file.toString(), file.length(), file.lastModified(), 0, lastBlock, 3);
            //assertTrue(result.get(0).remoteLocation == "CACHED", "File not cached properly");
            log.info(" Value of Result : " + result );


        }
        catch (TTransportException ex) {
            log.error("Error while creating bookkeeper client");
        }
        catch (TException ex) {
            log.error("Error while invoking getCacheStatus");
        }
    }
}
