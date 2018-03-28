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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;

import static com.qubole.rubix.core.utils.DataGen.getExpectedOutput;
import static com.qubole.rubix.core.utils.DataGen.populateFile;
import static org.testng.Assert.assertTrue;

/**
 * Created by stagra on 15/1/16.
 */
public class TestCachedReadRequestChain
{
  private static final Log log = LogFactory.getLog(TestCachedReadRequestChain.class);

  @Test
  public void testCachedReadRequestChain()
      throws IOException
  {
    String filename = "/tmp/testCachedReadRequestChainFile";
    populateFile(filename);

    File file = new File(filename);

    byte[] buffer = new byte[1000];
    ReadRequest[] readRequests = {
        new ReadRequest(0, 100, 0, 100, buffer, 0, file.length()),
        new ReadRequest(200, 300, 200, 300, buffer, 100, file.length()),
        new ReadRequest(400, 500, 400, 500, buffer, 200, file.length()),
        new ReadRequest(600, 700, 600, 700, buffer, 300, file.length()),
        new ReadRequest(800, 900, 800, 900, buffer, 400, file.length()),
        new ReadRequest(1000, 1100, 1000, 1100, buffer, 500, file.length()),
        new ReadRequest(1200, 1300, 1200, 1300, buffer, 600, file.length()),
        new ReadRequest(1400, 1500, 1400, 1500, buffer, 700, file.length()),
        new ReadRequest(1600, 1700, 1600, 1700, buffer, 800, file.length()),
        new ReadRequest(1800, 1900, 1800, 1900, buffer, 900, file.length())
    };

    CachedReadRequestChain cachedReadRequestChain = new CachedReadRequestChain(filename);
    for (ReadRequest rr : readRequests) {
      cachedReadRequestChain.addReadRequest(rr);
    }
    cachedReadRequestChain.lock();
    int readSize = cachedReadRequestChain.call();

    assertTrue(readSize == 1000, "Wrong amount of data read " + readSize);
    String output = new String(buffer, Charset.defaultCharset());
    assertTrue(getExpectedOutput(readSize).equals(output), "Wrong data read, expected\n" + getExpectedOutput(readSize) + "\nBut got\n" + output);

    file.delete();
  }
}
