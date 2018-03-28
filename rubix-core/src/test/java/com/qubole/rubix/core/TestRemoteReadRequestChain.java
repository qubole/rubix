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

import com.qubole.rubix.core.utils.DataGen;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.Charset;

import static org.testng.Assert.assertTrue;

/**
 * Created by stagra on 15/1/16.
 */
public class TestRemoteReadRequestChain
{
  FSDataInputStream fsDataInputStream;

  String backendFileName = "/tmp/testRemoteReadRequestChainBackendFile";
  File backendFile = new File(backendFileName);

  String localFileName = "/tmp/testRemoteReadRequestChainLocalFile";

  RemoteReadRequestChain remoteReadRequestChain;

  private static final Log log = LogFactory.getLog(TestRemoteReadRequestChain.class);

  @BeforeMethod
  public void setup()
      throws IOException
  {
    // Populate File
    DataGen.populateFile(backendFileName);

    LocalFSInputStream localFSInputStream = new LocalFSInputStream(backendFileName);
    fsDataInputStream = new FSDataInputStream(localFSInputStream);

    remoteReadRequestChain = new RemoteReadRequestChain(fsDataInputStream, localFileName);
  }

  @Test
  public void testBlockAlignedRead()
      throws IOException
  {
    byte[] buffer = new byte[1000];
    ReadRequest[] readRequests = {
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
    String generatedTestData = DataGen.getExpectedOutput(1000);

    testRead(readRequests,
        buffer,
        generatedTestData,
        generatedTestData);
  }

  @Test
  public void testBlockUnalignedRead()
      throws IOException
  {
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

    // Expected output is 50a100c100e....100q50s
    String generatedTestData = DataGen.getExpectedOutput(1000);
    String expectedBufferOutput = generatedTestData.substring(50, 950);
    testRead(readRequests,
        buffer,
        expectedBufferOutput,
        generatedTestData);
  }

  private void testRead(ReadRequest[] readRequests,
                        byte[] buffer,
                        String expectedBufferOutput,
                        String expectedCacheOutput)
      throws IOException
  {
    for (ReadRequest rr : readRequests) {
      remoteReadRequestChain.addReadRequest(rr);
    }

    remoteReadRequestChain.lock();

    // 2. Execute and verify that buffer has right data
    int readSize = remoteReadRequestChain.call();

    assertTrue(readSize == expectedBufferOutput.length(), "Wrong amount of data read " + readSize + " was expecting " + expectedBufferOutput.length());
    String actualBufferOutput = new String(buffer, Charset.defaultCharset());
    assertTrue(expectedBufferOutput.equals(actualBufferOutput), "Wrong data read, expected\n" + expectedBufferOutput + "\nBut got\n" + actualBufferOutput);

    // 3. read from randomAccessFile and verify that it has the right data
    // data present should be of form 100bytes of data and 100bytes of holes
    byte[] filledBuffer = new byte[expectedCacheOutput.length()];
    byte[] emptyBuffer = new byte[100];
    int filledBufferOffset = 0;
    readSize = 0;

    FileInputStream localFileInputStream = new FileInputStream(new File(localFileName));
    for (int i = 1; i < 20; i++) {
      //Expect a hole also in the case of partial prefix and suffix blocks.
      if (i % 2 == 0) {
        // empty buffer
        localFileInputStream.read(emptyBuffer, 0, 100);
        for (int j = 0; j < 100; j++) {
          assertTrue(emptyBuffer[j] == 0, "Got data instead of hole: " + emptyBuffer[j]);
        }
      }
      else {
        readSize += localFileInputStream.read(filledBuffer, filledBufferOffset, 100);
        filledBufferOffset += 100;
      }
    }
    localFileInputStream.close();
    log.debug("READ: \n" + new String(filledBuffer, Charset.defaultCharset()));
    assertTrue(readSize == expectedCacheOutput.length(), "Wrong amount of data read from localFile " + readSize);
    String actualCacheOutput = new String(filledBuffer, Charset.defaultCharset());
    assertTrue(expectedCacheOutput.equals(actualCacheOutput), "Wrong data read in local randomAccessFile, expected\n" + expectedCacheOutput + "\nBut got\n" + actualCacheOutput);
  }

  @AfterMethod
  public void cleanup()
      throws IOException
  {
    fsDataInputStream.close();
    backendFile.delete();
    File localFile = new File(localFileName);
    localFile.delete();
  }
}
