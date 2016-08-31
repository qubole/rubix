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
package com.qubole.rubix.core;

import com.qubole.rubix.bookkeeper.BookKeeperConfig;
import com.qubole.rubix.bookkeeper.BookKeeperServer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.testng.AssertJUnit.assertTrue;

/**
 * Created by stagra on 25/1/16.
 */
public class TestCachingInputStream
{
    int blockSize = 100;
    String backendFileName = "/tmp/backendFile";
    Path backendPath = new Path("file://" + backendFileName);

    CachingInputStream inputStream;

    private static final Log log = LogFactory.getLog(TestCachingInputStream.class);

    @BeforeMethod
    public void setup()
            throws IOException, InterruptedException
    {
        final Configuration conf = new Configuration();

        conf.setBoolean(CachingConfigHelper.DATA_CACHE_STRICT_MODE, true);
        conf.setInt(BookKeeperConfig.dataCacheBookkeeperPortConf, 3456);
        Thread thread = new Thread() {
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

    }

    public void createCachingStream(Configuration conf)
            throws InterruptedException, IOException
    {
        conf.setBoolean(CachingConfigHelper.DATA_CACHE_STRICT_MODE, true);
        conf.setInt(BookKeeperConfig.dataCacheBookkeeperPortConf, 3456);

        File file = new File(backendFileName);

        LocalFSInputStream localFSInputStream = new LocalFSInputStream(backendFileName);
        FSDataInputStream fsDataInputStream = new FSDataInputStream(localFSInputStream);
        conf.setInt(BookKeeperConfig.blockSizeConf, blockSize);

        log.info("All set to test");

        // This should be after server comes up else client could not be created
        inputStream = new CachingInputStream(fsDataInputStream, conf, backendPath, file.length(),file.lastModified(), new CachingFileSystemStats(), 64*1024*1024);

    }

    @AfterMethod
    public void cleanup()
    {
        BookKeeperServer.stopServer();
        Configuration conf = new Configuration();
        inputStream.close();
        File file = new File(backendFileName);
        file.delete();

        File mdFile = new File(BookKeeperConfig.getMDFile(backendPath.toString(), conf));
        mdFile.delete();

        File localFile = new File(BookKeeperConfig.getLocalPath(backendPath.toString(), conf));
        localFile.delete();
    }

    @Test
    public void testCaching()
            throws IOException
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
        assertions(readSize, 1000, buffer,expectedOutput);
    }

    @Test
    public void testChunkCachingAndEviction()
            throws IOException, InterruptedException
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

    private void writeZeros(String filename, int start, int end)
            throws IOException
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
    public void testEOF()
            throws IOException
    {
        inputStream.seek(2500);
        byte[] buffer = new byte[200];
        int readSize = inputStream.read(buffer, 0, 200);

        String expectedOutput = DataGen.generateContent().substring(2500);
        assertions(readSize, 100, Arrays.copyOf(buffer, readSize), expectedOutput);

        readSize = inputStream.read(buffer, 100, 100);
        assertTrue("Did not get EOF", readSize == -1);
    }

    private void assertions(int readSize, int expectedReadSize, byte[] outputBuffer, String expectedOutput)
    {
        assertTrue("Wrong amount of data read " + readSize + " was expecting " + expectedReadSize, readSize == expectedReadSize);
        String output = new String(outputBuffer, Charset.defaultCharset());
        assertTrue("Wrong data read, expected\n" + expectedOutput + "\nBut got\n" + output, expectedOutput.equals(output));
    }
}
