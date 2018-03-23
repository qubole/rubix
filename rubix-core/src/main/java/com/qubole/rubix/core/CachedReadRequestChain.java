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

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static com.google.common.base.Preconditions.checkState;

/**
 * Created by stagra on 4/1/16.
 */
public class CachedReadRequestChain extends ReadRequestChain
{
  private FileChannel fileChannel;
  private RandomAccessFile raf;
  private int read; // data read
  private FileSystem.Statistics statistics;

  private ByteBuffer directBuffer;

  private static final Log log = LogFactory.getLog(CachedReadRequestChain.class);

  public CachedReadRequestChain(String fileToRead, ByteBuffer buffer, FileSystem.Statistics statistics)
      throws IOException
  {
    this.raf = new RandomAccessFile(fileToRead, "r");
    FileInputStream fis = new FileInputStream(raf.getFD());
    fileChannel = fis.getChannel();
    directBuffer = buffer;
    this.statistics = statistics;
  }

  @VisibleForTesting
  public CachedReadRequestChain(String fileToRead)
      throws IOException
  {
    this(fileToRead, ByteBuffer.allocate(1024), null);
  }

  @VisibleForTesting
  public CachedReadRequestChain()
  {
    //Dummy constructor for testing #testConsequtiveRequest method.
  }

  public Integer call()
      throws IOException
  {
    // TODO: any exception here should not cause workload to fail
    // rather should be retried and eventually read from backend
    Thread.currentThread().setName(threadName);

    if (readRequests.size() == 0) {
      return 0;
    }

    checkState(isLocked, "Trying to execute Chain without locking");
    for (ReadRequest readRequest : readRequests) {
      if (cancelled) {
        propagateCancel(this.getClass().getName());
      }
      int nread = 0;
      int leftToRead = readRequest.getActualReadLength();
      log.debug(String.format("Processing readrequest %d-%d, length %d", readRequest.actualReadStart, readRequest.actualReadEnd, leftToRead));
      while (nread < readRequest.getActualReadLength()) {
        int readInThisCycle = Math.min(leftToRead, directBuffer.capacity());
        directBuffer.clear();
        int nbytes = fileChannel.read(directBuffer, readRequest.getActualReadStart() + nread);
        if (nbytes <= 0) {
          break;
        }
        directBuffer.flip();
        int transferBytes = Math.min(readInThisCycle, nbytes);
        directBuffer.get(readRequest.getDestBuffer(), readRequest.getDestBufferOffset() + nread, transferBytes);
        leftToRead -= transferBytes;
        nread += transferBytes;
      }
      log.debug(String.format("CachedFileRead copied data [%d - %d] at buffer offset %d",
          readRequest.getActualReadStart(),
          readRequest.getActualReadStart() + nread,
          readRequest.getDestBufferOffset()));
      read += nread;
    }
    log.info(String.format("Read %d bytes from cached file", read));
    fileChannel.close();
    raf.close();
    if (statistics != null) {
      statistics.incrementBytesRead(read);
    }
    return read;
  }

  public ReadRequestChainStats getStats()
  {
    return new ReadRequestChainStats()
        .setCachedDataRead(read)
        .setCachedReads(requests);
  }
}
