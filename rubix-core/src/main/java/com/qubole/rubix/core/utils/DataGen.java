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
package com.qubole.rubix.core.utils;

import org.apache.hadoop.util.DirectBufferPool;

import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Created by stagra on 21/1/16.
 */
public class DataGen
{
  private DataGen()
  {
  }

  public static String generateContent(int jump)
  {
    StringBuilder stringBuilder = new StringBuilder();
    for (char i = 'a'; i <= 'z'; i = (char) (i + jump)) {
      for (int j = 0; j < 100; j++) {
        stringBuilder.append(i);
      }
    }
    return stringBuilder.toString();
  }

  public static String generateContent()
  {
    return generateContent(1);
  }

  public static String getExpectedOutput(int size)
  {
    String expected = generateContent(2);
    return expected.substring(0, size);
  }

  public static void populateFile(String filename) throws IOException
  {
    PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(filename, false)));
    out.print(generateContent());
    out.close();
  }

  public static byte[] readBytesFromFile(String path, int offset, int length) throws IOException
  {
    RandomAccessFile raf = new RandomAccessFile(path, "r");
    FileInputStream fis = new FileInputStream(raf.getFD());
    DirectBufferPool bufferPool = new DirectBufferPool();
    ByteBuffer directBuffer = bufferPool.getBuffer(2000);
    byte[] result = new byte[length];
    FileChannel fileChannel = fis.getChannel();

    int nread = 0;
    int leftToRead = length;

    while (nread < length) {
      int readInThisCycle = Math.min(leftToRead, directBuffer.capacity());
      directBuffer.clear();
      int nbytes = fileChannel.read(directBuffer, offset + nread);
      if (nbytes <= 0) {
        break;
      }

      directBuffer.flip();
      int transferBytes = Math.min(readInThisCycle, nbytes);
      directBuffer.get(result, nread, transferBytes);
      leftToRead -= transferBytes;
      nread += transferBytes;
    }

    return result;
  }
}
