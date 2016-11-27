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

/**
 * Created by sakshia on 25/11/16.
 */

import com.qubole.rubix.bookkeeper.BookKeeperServer;
import com.qubole.rubix.bookkeeper.LocalDataTransferServer;
import com.qubole.rubix.core.DataGen;
import com.qubole.rubix.core.NonLocalReadRequestChain;
import com.qubole.rubix.core.ReadRequest;
import com.qubole.rubix.core.TestCachingFileSystem;
import com.qubole.rubix.spi.CacheConfig;
import com.qubole.rubix.spi.ClusterType;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.BasicConfigurator;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;

import static org.testng.AssertJUnit.assertTrue;

public class TestNonLocalReadRequestChain
{
    int blockSize = 100;
    String backendFileName = "/tmp/backendFile";
    Path backendPath = new Path("testfile:/" + backendFileName);
    File backendFile = new File(backendFileName);
    final Configuration conf = new Configuration();

    NonLocalReadRequestChain nonLocalReadRequestChain;
    private static final Log log = LogFactory.getLog(TestNonLocalReadRequestChain.class);

    @BeforeMethod
    public void setup()
            throws Exception
    {
        conf.setBoolean(CacheConfig.DATA_CACHE_STRICT_MODE, true);
        conf.setInt(CacheConfig.dataCacheBookkeeperPortConf, 3456);
        conf.setInt(CacheConfig.localServerPortConf, 2222);
        conf.setInt(CacheConfig.blockSizeConf, blockSize);
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

        while (!BookKeeperServer.isServerUp()) {
            Thread.sleep(200);
            log.info("Waiting for BookKeeper Server to come up");
        }

        // Populate File
        DataGen.populateFile(backendFileName);

        conf.setClass("fs.testfile.impl", TestCachingFileSystem.class, FileSystem.class);
        nonLocalReadRequestChain = new NonLocalReadRequestChain("localhost", backendFile.length(), backendFile.lastModified(), conf, null, backendPath.toString(), ClusterType.TEST_CLUSTER_MANAGER.ordinal());
    }

    @Test
    private void testRead()
            throws Exception
    {
        BasicConfigurator.configure();
        byte[] buffer = new byte[900];

        ReadRequest[] readRequests = {
                new ReadRequest(0, 100, 50, 100, buffer, 0, backendFile.length()),
                new ReadRequest(200, 300, 200, 300, buffer, 50, backendFile.length()),
                new ReadRequest(400, 500, 400, 500, buffer, 150, backendFile.length()),
                new ReadRequest(600, 700, 600, 700, buffer, 250, backendFile.length()),
                new ReadRequest(800, 900, 800, 900, buffer, 350, backendFile.length()),
                new ReadRequest(1000, 1100, 1000, 1100, buffer, 450, backendFile.length()),
                new ReadRequest(1200, 1300, 1200, 1300, buffer, 550, backendFile.length()),
                new ReadRequest(1400, 1500, 1400, 1500, buffer, 650, backendFile.length()),
                new ReadRequest(1600, 1700, 1600, 1700, buffer, 750, backendFile.length()),
                new ReadRequest(1800, 1900, 1800, 1850, buffer, 850, backendFile.length())
        };

        //2. send non-local readrequest
        for (ReadRequest rr : readRequests) {
            nonLocalReadRequestChain.addReadRequest(rr);
        }

        nonLocalReadRequestChain.lock();

        // 2. Execute and verify that buffer has right data
        int readSize = nonLocalReadRequestChain.call();

        assertTrue("Wrong amount of data read " + readSize + " was expecting " + 900, readSize == 900);
        String output = new String(buffer, Charset.defaultCharset());
        String expectedOutput = DataGen.getExpectedOutput(1000).substring(50, 950);
        assertTrue("Wrong data read, expected\n" + expectedOutput + "\nBut got\n" + output, expectedOutput.equals(output));
 }

    @AfterMethod
    public void cleanup()
            throws IOException
    {
        BookKeeperServer.stopServer();
        LocalDataTransferServer.stopServer();

        File mdFile = new File(CacheConfig.getMDFile(backendPath.toString(), conf));
        mdFile.delete();

        File localFile = new File(CacheConfig.getLocalPath(backendPath.toString(), conf));
        localFile.delete();
        backendFile.delete();
    }

}

