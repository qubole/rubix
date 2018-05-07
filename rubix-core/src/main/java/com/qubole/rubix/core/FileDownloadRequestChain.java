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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static com.google.common.base.Preconditions.checkState;

public class FileDownloadRequestChain extends ReadRequestChain
{
  private FileSystem remoteFileSystem;
  private String localFile;
  private String remotePath;
  private long fileSize;
  private long lastModified;
  private BookKeeperFactory bookKeeperFactory;
  private int totalRequestedRead;
  private int warmupPenalty;
  private int blockSize;
  Configuration conf;
  ByteBuffer directBuffer;

  private static final Log log = LogFactory.getLog(FileDownloadRequestChain.class);

  public FileDownloadRequestChain(FileSystem remoteFileSystem, String localfile, ByteBuffer directBuffer,
                                  Configuration conf, String remotePath, long fileSize, long lastModified)
  {
    this.remoteFileSystem = remoteFileSystem;
    this.localFile = localfile;
    this.conf = conf;
    this.remotePath = remotePath;
    this.fileSize = fileSize;
    this.lastModified = lastModified;
    this.bookKeeperFactory = new BookKeeperFactory();
    this.blockSize = CacheConfig.getBlockSize(conf);
    this.directBuffer = directBuffer;
  }

  public String getRemotePath()
  {
    return this.remotePath;
  }

  public long getFileSize()
  {
    return this.fileSize;
  }

  public long getLastModified()
  {
    return this.lastModified;
  }

  public Integer call() throws IOException
  {
    Thread.currentThread().setName(threadName);
    checkState(isLocked, "Trying to execute Chain without locking");
    long startTime = System.currentTimeMillis();

    if (readRequests.size() == 0) {
      return 0;
    }

    File file = new File(localFile);
    if (!file.exists()) {
      log.info("Creating localfile : " + localFile);
      file.createNewFile();
      file.setWritable(true, false);
      file.setReadable(true, false);
    }

    FSDataInputStream inputStream = null;
    FileChannel fileChannel = null;

    try {
      inputStream = remoteFileSystem.open(new Path(remotePath), CacheConfig.getBlockSize(conf));
      fileChannel = new FileOutputStream(new RandomAccessFile(file, "rw").getFD()).getChannel();
      for (ReadRequest readRequest : readRequests) {
        if (cancelled) {
          log.info("Request Cancelled for " + readRequest.backendReadStart);
          propagateCancel(this.getClass().getName());
        }

        int readBytes = 0;
        inputStream.seek(readRequest.backendReadStart);
        log.info("Seeking to " + readRequest.backendReadStart);
        //log.info("Processing request of  " + readRequest.getBackendReadLength() + " from " + readRequest.backendReadStart);
        readBytes = copyIntoCache(inputStream, fileChannel, readRequest.getBackendReadLength(),
            readRequest.backendReadStart);
        totalRequestedRead += readBytes;
      }
      log.info("Downloaded " + totalRequestedRead + " bytes of file " + remotePath);
      log.debug("RemoteFetchRequest took : " + (System.currentTimeMillis() - startTime) + " msecs ");
      return 0;
    }
    finally {
      fileChannel.close();
      inputStream.close();
    }
  }

  private int copyIntoCache(FSDataInputStream inputStream, FileChannel fileChannel, int length,
                            long cacheReadStart) throws IOException
  {
    byte[] buffer = null;
    int nread = 0;
    try {
      long start = System.nanoTime();
      buffer = new byte[length];
      log.info("Copying data of file " + remotePath + " of length " + length + " from offset " + cacheReadStart);
      while (nread < length) {
        int nbytes = inputStream.read(buffer, nread, length - nread);
        if (nbytes < 0) {
          break;
        }
        nread += nbytes;
      }

      int leftToWrite = length;
      int writtenSoFar = 0;

      while (leftToWrite > 0) {
        int writeInThisCycle = Math.min(leftToWrite, directBuffer.capacity());
        directBuffer.clear();
        directBuffer.put(buffer, writtenSoFar, writeInThisCycle);
        directBuffer.flip();
        int nwrite = fileChannel.write(directBuffer, cacheReadStart + writtenSoFar);
        directBuffer.compact();
        writtenSoFar += nwrite;
        leftToWrite -= nwrite;
      }
      warmupPenalty += System.nanoTime() - start;
      log.info("Read " + nread + " for file " + remotePath + " from offset " + cacheReadStart);
    }
    finally {
      buffer = null;
    }
    return nread;
  }

  @Override
  public void updateCacheStatus(String remotePath, long fileSize, long lastModified, int blockSize, Configuration conf)
  {
    try {
      log.info("Updating cache for FileDownloadRequestChain . Num Requests : " + readRequests.size() + " for remotepath : " + remotePath);
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
