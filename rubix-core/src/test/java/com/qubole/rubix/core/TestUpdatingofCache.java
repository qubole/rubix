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
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.io.file.tfile.ByteArray;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.testng.AssertJUnit.assertTrue;

/**
 * Created by qubole on 19/7/16 .
 */
public class TestUpdatingofCache
{
    int blockSize = 100;
    String backendFileName = "/tmp/backendFile";
    Path backendPath = new Path("file://" + backendFileName);

    CachingInputStream inputStream;

    private static final Log log = LogFactory.getLog(TestUpdatingofCache.class);

    @BeforeMethod
    public void setup_initial()
            throws IOException, InterruptedException
    {
        final Configuration conf = new Configuration();
        conf.setBoolean(CachingConfigHelper.DATA_CACHE_STRICT_MODE, true);
        conf.setInt(BookKeeperConfig.DATA_CACHE_BOOKKEEPER_PORT, 3456);
        Thread thread = new Thread() {
            public void run()
            {
                BookKeeperServer.startServer(conf);
            }
        };
        thread.start();

        DataGen.populateFile(backendFileName);
        LocalFSInputStream localFSInputStream = new LocalFSInputStream(backendFileName);
        FSDataInputStream fsDataInputStream = new FSDataInputStream(localFSInputStream);

        File file = new File(backendFileName);
        conf.setInt(BookKeeperConfig.BLOCK_SIZE, blockSize);

        while (!BookKeeperServer.isServerUp()) {
            Thread.sleep(200);
            log.info("Waiting for BookKeeper Server to come up");
        }
        log.info("All set to test");

        // This should be after server comes up else client could not be created
        inputStream = new CachingInputStream(fsDataInputStream, conf, backendPath, file.length(),file.lastModified(), new CachingFileSystemStats());
    }

    public void setup()
            throws InterruptedException, IOException
    {
        final Configuration conf = new Configuration();
        conf.setBoolean(CachingConfigHelper.DATA_CACHE_STRICT_MODE, true);
        conf.setInt(BookKeeperConfig.DATA_CACHE_BOOKKEEPER_PORT, 3456);

        File file = new File(backendFileName);

        LocalFSInputStream localFSInputStream = new LocalFSInputStream(backendFileName);
        FSDataInputStream fsDataInputStream = new FSDataInputStream(localFSInputStream);
        conf.setInt(BookKeeperConfig.BLOCK_SIZE, blockSize);

        log.info("All set to test 1");

        // This should be after server comes up else client could not be created
        inputStream = new CachingInputStream(fsDataInputStream, conf, backendPath, file.length(),file.lastModified(), new CachingFileSystemStats());


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

    private void testEvictionHelper()
            throws IOException
    {
        inputStream.seek(100);
        byte[] buffer = new byte[1000];
        int readSize = inputStream.read(buffer, 0, 1000);
        String expectedOutput = DataGen.generateContent().substring(100, 1100);
        assertions(readSize, 1000, buffer,expectedOutput);
    }

    @Test
    public void testEviction()
            throws IOException, InterruptedException
    {

        // 1. Seek and read some data
        testEvictionHelper();

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
        writeZeros(backendFileName,100, 1100);
        writeZeros(backendFileName, 1550, 1750);

        // 5. Read from [0, 1750) and verify that old data is returned since lastModifiedDate remains the same
        Thread.sleep(3000);
        buffer = new byte[1750];
        inputStream.seek(0);
        readSize = inputStream.read(buffer, 0, 1750);
        expectedOutput = DataGen.generateContent().substring(0, 1750);
        assertions(readSize, 1750, buffer, expectedOutput);

        //6. Close existing stream and start a new one to get the new lastModifiedDate of backend file
        inputStream.close();
        setup();
        log.info("New stream started");

        //7. Read the data again and verify that correct, updated data is being read from the backend file and that the previous cache entry is evicted.
        inputStream.seek(100);
        buffer = new byte[1000];
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

    private void assertions(int readSize, int expectedReadSize, byte[] outputBuffer, String expectedOutput)
            throws UnsupportedEncodingException
    {
        assertTrue("Wrong amount of data read " + readSize + " was expecting " + expectedReadSize, readSize == expectedReadSize);
        String output = new String(outputBuffer,"UTF-8");
        assertTrue("Wrong data read, expected\n" + expectedOutput + "\nBut got\n" + output, expectedOutput.equals(output));
    }

}
