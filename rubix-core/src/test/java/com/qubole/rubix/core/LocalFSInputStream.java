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

import org.apache.hadoop.fs.FSInputStream;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Created by stagra on 21/1/16.
 */
public class LocalFSInputStream extends FSInputStream
{
  FileInputStream fis;
  private long position;

  public LocalFSInputStream(String f) throws IOException
  {
    this.fis = new FileInputStream(f);
  }

  public void seek(long pos) throws IOException
  {
    fis.getChannel().position(pos);
    this.position = pos;
  }

  public long getPos() throws IOException
  {
    return this.position;
  }

  public boolean seekToNewSource(long targetPos) throws IOException
  {
    return false;
  }

  /*
   * Just forward to the fis
   */
  public int available() throws IOException
  {
    return fis.available();
  }

  public void close() throws IOException
  {
    fis.close();
  }

  public boolean markSupport()
  {
    return false;
  }

  public int read() throws IOException
  {
    int value = fis.read();
    if (value >= 0) {
      this.position++;
    }
    return value;
  }

  public int read(byte[] b, int off, int len) throws IOException
  {
    int value = fis.read(b, off, len);
    if (value > 0) {
      this.position += value;
    }
    return value;
  }

  public int read(long position, byte[] b, int off, int len)
      throws IOException
  {
    ByteBuffer bb = ByteBuffer.wrap(b, off, len);
    return fis.getChannel().read(bb, position);
  }

  public long skip(long n) throws IOException
  {
    long value = fis.skip(n);
    if (value > 0) {
      this.position += value;
    }
    return value;
  }
}
