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

import com.google.common.base.Throwables;
import com.qubole.rubix.core.ReadRequest;
import com.qubole.rubix.core.ReadRequestChain;
import com.qubole.rubix.core.ReadRequestChainStats;
import com.qubole.rubix.spi.CacheConfig;
import com.qubole.rubix.spi.CacheUtil;
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
import java.util.List;

import static com.google.common.base.Preconditions.checkState;

public class FileDownloadRequestChain extends ReadRequestChain
{
  private BookKeeper bookKeeper;
  private FileSystem remoteFileSystem;
  private String localFile;
  private String remotePath;
  private long fileSize;
  private long lastModified;
  private int totalRequestedRead;
  private int warmupPenalty;
  private int blockSize;
  Configuration conf;
  ByteBuffer directBuffer;
  private long timeSpentOnDownload;

  private static final Log log = LogFactory.getLog(FileDownloadRequestChain.class);

  public FileDownloadRequestChain(BookKeeper bookKeeper, FileSystem remoteFileSystem, String localfile,
                                  ByteBuffer directBuffer, Configuration conf, String remotePath,
                                  long fileSize, long lastModified)
  {
    this.bookKeeper = bookKeeper;
    this.remoteFileSystem = remoteFileSystem;
    this.localFile = localfile;
    this.conf = conf;
    this.remotePath = remotePath;
    this.fileSize = fileSize;
    this.lastModified = lastModified;
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

  public long getTimeSpentOnDownload()
  {
    return this.timeSpentOnDownload;
  }

  public Integer call() throws IOException
  {
    log.debug(String.format("Read Request threadName: %s, FileDownload Executor threadName: %s", threadName, Thread.currentThread().getName()));
    Thread.currentThread().setName(threadName);
    checkState(isLocked(), "Trying to execute Chain without locking");

    List<ReadRequest> readRequests = getReadRequests();

    if (readRequests.size() == 0) {
      return 0;
    }

    long startTime = System.currentTimeMillis();
    File file = new File(localFile);
    if (!file.exists()) {
      log.debug("Creating localfile : " + localFile);
      String metadataFilePath = CacheUtil.getMetadataFilePath(remotePath, conf);
      File mdFile = new File(metadataFilePath);
      if (mdFile.exists() && mdFile.length() > 0) {
        // Making sure when a new file gets created, we invalidate the existing metadata file
        bookKeeper.invalidateFileMetadata(remotePath);
      }
      file.setWritable(true, false);
      file.setReadable(true, false);
      file.createNewFile();
    }

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

        int readBytes = 0;
        inputStream.seek(readRequest.getBackendReadStart());
        log.debug("Seeking to " + readRequest.getBackendReadStart());
        readBytes = copyIntoCache(inputStream, fileChannel, readRequest.getBackendReadLength(),
            readRequest.getBackendReadStart());
        totalRequestedRead += readBytes;
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

      fileSystem.close();
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
      log.debug("Copying data of file " + remotePath + " of length " + length + " from offset " + cacheReadStart);
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
      log.debug("Read " + nread + " for file " + remotePath + " from offset " + cacheReadStart);
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
      log.debug("Updating cache for FileDownloadRequestChain . Num Requests : " + getReadRequests().size() + " for remotepath : " + remotePath);
      for (ReadRequest readRequest : getReadRequests()) {
        log.debug("Setting cached from : " + toBlock(readRequest.getBackendReadStart()) + " block to : " + (toBlock(readRequest.getBackendReadEnd() - 1) + 1));
        bookKeeper.setAllCached(remotePath, fileSize, lastModified, toBlock(readRequest.getBackendReadStart()), toBlock(readRequest.getBackendReadEnd() - 1) + 1);
      }
    }
    catch (Exception e) {
      log.debug("Could not update BookKeeper about newly cached blocks: " + Throwables.getStackTraceAsString(e));
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
