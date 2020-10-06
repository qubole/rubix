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
package com.qubole.rubix.bookkeeper;

import com.qubole.rubix.core.ReadRequest;
import com.qubole.rubix.core.ReadRequestChain;
import com.qubole.rubix.core.ReadRequestChainStats;
import com.qubole.rubix.spi.CacheConfig;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static com.qubole.rubix.core.RemoteReadRequestChain.readIntoBuffer;
import static com.qubole.rubix.spi.CommonUtilities.toEndBlock;
import static com.qubole.rubix.spi.CommonUtilities.toStartBlock;

public class FileDownloadRequestChain extends ReadRequestChain
{
  private BookKeeper bookKeeper;
  private FileSystem remoteFileSystem;
  private String localFile;
  private String remotePath;
  private long fileSize;
  private long lastModified;
  private long totalRequestedRead;
  private final int maxRemoteReadBufferSize;
  Configuration conf;
  ByteBuffer directBuffer;
  private long timeSpentOnDownload;
  private int blockSize;

  private static final Log log = LogFactory.getLog(FileDownloadRequestChain.class);

  public FileDownloadRequestChain(BookKeeper bookKeeper,
      FileSystem remoteFileSystem,
      String localfile,
      ByteBuffer directBuffer,
      Configuration conf,
      String remotePath,
      long fileSize,
      long lastModified,
      int generationNumber)
  {
    super(generationNumber, getBlockAlignedMaxChunkSize(conf));
    this.bookKeeper = bookKeeper;
    this.remoteFileSystem = remoteFileSystem;
    this.localFile = localfile;
    this.conf = conf;
    this.remotePath = remotePath;
    this.fileSize = fileSize;
    this.lastModified = lastModified;
    this.directBuffer = directBuffer;
    this.maxRemoteReadBufferSize = CacheConfig.getDataTransferBufferSize(conf);
    this.blockSize = CacheConfig.getBlockSize(conf);
  }

  private static long getBlockAlignedMaxChunkSize(Configuration conf)
  {
    long maxReadRequestLength = CacheConfig.getParallelWarmupMaxChunkSize(conf);
    long blockSize = CacheConfig.getBlockSize(conf);
    return (maxReadRequestLength / blockSize) * blockSize;
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

  public long getTimeSpentOnDownload()
  {
    return this.timeSpentOnDownload;
  }

  @Override
  public Long call() throws IOException
  {
    checkState(isLocked(), "Trying to execute Chain without locking");

    List<ReadRequest> readRequests = getReadRequests();

    if (readRequests.size() == 0) {
      return 0L;
    }

    long startTime = System.currentTimeMillis();
    File file = new File(localFile);
    if (!file.exists()) {
      throw new FileNotFoundException(String.format("File does not exists %s", localFile));
    }

    long highestReadRequestLength = readRequests
            .stream()
            .map(readRequest -> readRequest.getActualReadLength())
            .max(Long::compareTo)
            .get();
    int remoteReadBufferSize = Math.min(maxRemoteReadBufferSize,
            Math.toIntExact(Math.min(Integer.MAX_VALUE, highestReadRequestLength)));
    byte[] remoteReadBuffer = new byte[remoteReadBufferSize];

    FSDataInputStream inputStream = null;
    FileChannel fileChannel = null;
    FileSystem fileSystem = remoteFileSystem;
    try {
      inputStream = fileSystem.open(new Path(remotePath), CacheConfig.getBlockSize(conf));
      fileChannel = new FileOutputStream(new RandomAccessFile(file, "rw").getFD()).getChannel();
      for (ReadRequest readRequest : readRequests) {
        if (isCancelled()) {
          log.debug("Request Cancelled for " + readRequest.getBackendReadStart());
          propagateCancel(this.getClass().getName());
        }

        long readBytes = copyIntoCache(inputStream, fileChannel, readRequest.getBackendReadStart(), readRequest.getBackendReadLength(), remoteReadBuffer);
        totalRequestedRead += readBytes;

        // Update BookKeeper about the downloaded data asap to minimze the errors in disk space accounting
        updateCacheStatus(readRequest);
      }
      long endTime = System.currentTimeMillis();
      timeSpentOnDownload = (endTime - startTime) / 1000;

      log.debug("Downloaded " + totalRequestedRead + " bytes of file " + remotePath);
      log.debug("RemoteFetchRequest took : " + timeSpentOnDownload + " secs ");
      return totalRequestedRead;
    }
    finally {
      if (fileChannel != null) {
        fileChannel.close();
      }

      if (inputStream != null) {
        inputStream.close();
      }
    }
  }

  private long copyIntoCache(FSDataInputStream inputStream,
          FileChannel fileChannel,
          long cacheReadStart,
          long length,
          byte[] remoteReadBuffer) throws IOException
  {
    log.debug(String.format("Copying data of file %s of length %d from position %d", remotePath, length, cacheReadStart));
    if (length <= remoteReadBuffer.length) {
      inputStream.readFully(cacheReadStart, remoteReadBuffer, 0, Math.toIntExact(length));
      writeToFile(remoteReadBuffer, Math.toIntExact(length), fileChannel, cacheReadStart);
    }
    else {
      // Use streaming reads here as we will be doing multiple iterations
      long leftToRead = length;
      while (leftToRead > 0) {
        int toRead = Math.toIntExact(Math.min(remoteReadBuffer.length, leftToRead));
        inputStream.seek(cacheReadStart);
        readIntoBuffer(remoteReadBuffer, 0, toRead, inputStream);
        writeToFile(remoteReadBuffer, toRead, fileChannel, cacheReadStart);
        cacheReadStart += toRead;
        leftToRead -= toRead;
      }
    }

    log.debug(String.format("Copied %d to file %s from position %d", length, remotePath, cacheReadStart));
    return length;
  }

  private void writeToFile(byte[] buffer, int length, FileChannel fileChannel, long cacheReadStart)
          throws IOException
  {
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
  }

  private void updateCacheStatus(ReadRequest readRequest)
  {
    long startBlock = toStartBlock(readRequest.getBackendReadStart(), blockSize);
    long endBlock = toEndBlock(readRequest.getBackendReadEnd(), blockSize);
    try {
      bookKeeper.setAllCached(remotePath,
              fileSize,
              lastModified,
              startBlock,
              endBlock,
              generationNumber);
    }
    catch (Exception e) {
      log.warn(String.format("Unable to update cache status for %s:%d:%d, this can cause wrong accounting of disk utilization",
              remotePath,
              startBlock,
              endBlock),
              e);
    }
  }

  public ReadRequestChainStats getStats()
  {
    // Update same stats that RemoteRRC updates
    return new ReadRequestChainStats()
            .setRemoteRRCDataRead(totalRequestedRead)
            .setRemoteRRCWarmupTime(timeSpentOnDownload);
  }
}
