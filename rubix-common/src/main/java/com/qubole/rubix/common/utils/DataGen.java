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
package com.qubole.rubix.common.utils;

import org.apache.hadoop.util.DirectBufferPool;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;

/**
 * Created by stagra on 21/1/16.
 */
public class DataGen
{
  private DataGen()
  {
  }

  public static String generateContent(int jump, int sizeMultiplier)
  {
    StringBuilder stringBuilder = new StringBuilder();
    for (char i = 'a'; i <= 'z'; i = (char) (i + jump)) {
      for (int j = 0; j < sizeMultiplier; j++) {
        stringBuilder.append(i);
      }
    }
    return stringBuilder.toString();
  }

  public static String generateContent(int jump)
  {
    return generateContent(jump, 100);
  }

  public static String generateContent()
  {
    return generateContent(1);
  }

  public static String getExpectedOutput(int size, int sizeMultiplier)
  {
    String expected = generateContent(2, sizeMultiplier);
    return expected.substring(0, size);
  }

  public static String getExpectedOutput(long size)
  {
    return getExpectedOutput(Math.toIntExact(size), 100);
  }

  public static void populateFile(String filename) throws IOException
  {
    PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(filename, false)));
    out.print(generateContent());
    out.close();
  }

  public static void populateFile(String filename, int skip) throws IOException
  {
    PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(filename, false)));
    out.print(generateContent(skip));
    out.close();
  }

  public static long populateFile(String filename, int skip, int sizeMultiplier) throws IOException
  {
    PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(filename, false)));
    String data = generateContent(skip, sizeMultiplier);
    out.print(data);
    out.close();
    return data.length();
  }

  public static void writeZerosInFile(String filename, int start, int end) throws IOException
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

  public static void truncateFile(String fileName, int newSize)
  {
    try (FileChannel fileChannel = new FileOutputStream(fileName, true).getChannel()) {
      fileChannel.truncate(newSize);
    }
    catch (IOException e) {
      //Ignore
    }
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
