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

import com.google.common.base.Throwables;
import com.qubole.rubix.spi.BookKeeperFactory;
import com.qubole.rubix.spi.CacheConfig;
import com.qubole.rubix.spi.RetryingBookkeeperClient;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static com.google.common.base.Preconditions.checkState;

public class RemoteFetchRequestChain extends ReadRequestChain
{

  private FSDataInputStream inputStream;
  private String localFile;
  private BookKeeperFactory bookKeeperFactory;
  private int totalRequestedRead = 0;
  private int warmupPenalty = 0;
  private int blockSize = 0;
  Configuration conf;
  ByteBuffer directBuffer;

  private static final Log log = LogFactory.getLog(RemoteFetchRequestChain.class);


  public RemoteFetchRequestChain(FSDataInputStream inputStream, String localfile, ByteBuffer directBuffer, Configuration conf) throws IOException {
    this.inputStream = inputStream;
    this.localFile = localfile;
    this.conf = conf;
    this.bookKeeperFactory = new BookKeeperFactory();
    this.blockSize = CacheConfig.getBlockSize(conf);
    this.directBuffer = directBuffer;
  }

  public Integer call() throws IOException {
    Thread.currentThread().setName(threadName);
    checkState(isLocked, "Trying to execute Chain without locking");

    if (readRequests.size() == 0) {
      return 0;
    }

    File file = new File(localFile);
    if (!file.exists()) {
      file.createNewFile();
      file.setWritable(true, false);
      file.setReadable(true, false);
    }

    FileChannel fileChannel = new FileOutputStream(new RandomAccessFile(file, "rw").getFD()).getChannel();
    try {
      for (ReadRequest readRequest : readRequests) {
        if (cancelled) {
          propagateCancel(this.getClass().getName());
        }

        int readBytes = 0;
        inputStream.seek(readRequest.backendReadStart);
        //log.info("Processing request of  " + readRequest.getBackendReadLength() + " from " + readRequest.backendReadStart);
        readBytes = copyIntoCache(fileChannel, readRequest.getBackendReadLength(), readRequest.backendReadStart);
        totalRequestedRead += readBytes;
      }
      return 0;
    } finally {
      fileChannel.close();
    }
  }

  private int copyIntoCache(FileChannel fileChannel, int length, long cacheReadStart) throws IOException {
    long start = System.nanoTime();
    int nread = 0;
    byte[] buffer = new byte[CacheConfig.getDiskReadBufferSizeDefault(conf)];
    while (nread < length) {
      //log.info("Reading data Offset: " + nread + " of length : " + (length - nread));
      int nbytes = inputStream.read(buffer, nread, length - nread);
      //log.info("Read data : " + nbytes);
      if (nbytes < 0)
        break;

      directBuffer.clear();
      directBuffer.put(buffer, nread, nbytes);
      directBuffer.flip();
      fileChannel.write(directBuffer, cacheReadStart + nread);
      directBuffer.compact();
      nread += nbytes;
    }
    warmupPenalty += System.nanoTime() - start;
    return nread;
  }

  @Override
  public void updateCacheStatus(String remotePath, long fileSize, long lastModified, int blockSize, Configuration conf)
  {
    try {
      log.info("Updating cache for RemoteFetchRequestChain . Num Requests : " + readRequests.size());
      RetryingBookkeeperClient client = bookKeeperFactory.createBookKeeperClient(conf);
      for (ReadRequest readRequest : readRequests) {
        log.debug("Setting cached from : " + toBlock(readRequest.getBackendReadStart()) + " block to : " + (toBlock(readRequest.getBackendReadEnd() - 1) + 1));
        client.setAllCached(remotePath, fileSize, lastModified, toBlock(readRequest.getBackendReadStart()), toBlock(readRequest.getBackendReadEnd() - 1) + 1);
      }
      client.close();
    }
    catch (Exception e) {
      log.info("Could not update BookKeeper about newly cached blocks: " + Throwables.getStackTraceAsString(e));
    }
  }

  public ReadRequestChainStats getStats()
  {
    return new ReadRequestChainStats()
        .setRequestedRead(totalRequestedRead)
        .setWarmupPenalty(warmupPenalty)
        .setRemoteReads(requests);
  }

  private long toBlock(long pos)
  {
    return pos / blockSize;
  }

}