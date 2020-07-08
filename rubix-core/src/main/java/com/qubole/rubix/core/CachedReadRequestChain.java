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
package com.qubole.rubix.core;

import com.google.common.annotations.VisibleForTesting;
import com.qubole.rubix.spi.BookKeeperFactory;
import com.qubole.rubix.spi.CacheUtil;
import com.qubole.rubix.spi.RetryingPooledBookkeeperClient;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.DirectBufferPool;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static com.google.common.base.Preconditions.checkState;
import static com.qubole.rubix.spi.CacheUtil.UNKONWN_GENERATION_NUMBER;

/**
 * Created by stagra on 4/1/16.
 */
public class CachedReadRequestChain extends ReadRequestChain
{
  private String remotePath;
  private long read; // data read
  private FileSystem.Statistics statistics;
  private FileSystem remoteFileSystem;
  private DirectReadRequestChain directReadChain;
  private Configuration conf;
  private long directDataRead;
  private BookKeeperFactory factory;

  private DirectBufferPool bufferPool;
  private int directBufferSize;
  private int corruptedFileCount;

  private static final Log log = LogFactory.getLog(CachedReadRequestChain.class);

  public CachedReadRequestChain(FileSystem remoteFileSystem,
      String remotePath,
      DirectBufferPool bufferPool,
      int directBufferSize,
      FileSystem.Statistics statistics,
      Configuration conf,
      BookKeeperFactory factory,
      int generationNumber)
  {
    super(generationNumber);
    this.conf = conf;
    this.remotePath = remotePath;
    this.remoteFileSystem = remoteFileSystem;
    this.bufferPool = bufferPool;
    this.directBufferSize = directBufferSize;
    this.statistics = statistics;
    this.factory = factory;
  }

  @VisibleForTesting
  public CachedReadRequestChain(FileSystem remoteFileSystem, String remotePath, Configuration conf, BookKeeperFactory factory, int generationNumber)
  {
    this(remoteFileSystem, remotePath, new DirectBufferPool(), 100, null, conf, factory, generationNumber);
  }

  @VisibleForTesting
  public CachedReadRequestChain()
  {
    //Dummy constructor for testing #testConsequtiveRequest method.
    super(UNKONWN_GENERATION_NUMBER);
  }

  public Long call() throws IOException
  {
    // TODO: any exception here should not cause workload to fail
    // rather should be retried and eventually read from backend
    log.debug(String.format("Read Request threadName: %s, Cached read Executor threadName: %s", threadName, Thread.currentThread().getName()));
    Thread.currentThread().setName(threadName);

    if (readRequests.size() == 0) {
      return 0L;
    }

    checkState(isLocked, "Trying to execute Chain without locking");

    RandomAccessFile raf = null;
    FileInputStream fis = null;
    FileChannel fileChannel = null;
    boolean needsInvalidation = false;
    String localCachedFile = CacheUtil.getLocalPath(remotePath, conf, generationNumber);

    ByteBuffer directBuffer = bufferPool.getBuffer(directBufferSize);
    try {
      raf = new RandomAccessFile(localCachedFile, "r");
      fis = new FileInputStream(raf.getFD());
      fileChannel = fis.getChannel();

      for (ReadRequest readRequest : readRequests) {
        if (cancelled) {
          propagateCancel(this.getClass().getName());
        }
        int nread = 0;
        int leftToRead = readRequest.getActualReadLengthIntUnsafe();
        log.debug(String.format("Processing readrequest %d-%d, length %d", readRequest.actualReadStart, readRequest.actualReadEnd, leftToRead));
        while (nread < readRequest.getActualReadLengthIntUnsafe()) {
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

        if (nread != readRequest.getActualReadLengthIntUnsafe()) {
          throw new InvalidationRequiredException("Cached read length didn't match with requested read length for file");
        }
        else {
          read += nread;
        }
      }
      log.debug(String.format("Read %d bytes from cached file", read));
    }
    catch (Exception ex) {
      if (ex instanceof CancelledException) {
        throw ex;
      }

      log.error(String.format("Fall back to read from object store for %s .Could not read data from cached file : ", localCachedFile), ex);
      needsInvalidation = true;
      directDataRead = readFromRemoteFileSystem();
      return directDataRead;
    }
    finally {
      bufferPool.returnBuffer(directBuffer);

      if (fis != null) {
        fis.close();
      }
      if (fileChannel != null) {
        fileChannel.close();
      }
      if (raf != null) {
        raf.close();
      }

      // We are calling invalidateMetadata from finally block to make sure fileChannel is closed before we delete the file
      if (needsInvalidation) {
        corruptedFileCount++;
        invalidateMetadata();
      }

      if (statistics != null) {
        statistics.incrementBytesRead(read);
      }
    }
    return read;
  }

  @Override
  public void cancel()
  {
    super.cancel();
    if (directReadChain != null) {
      directReadChain.cancel();
    }
  }

  private void invalidateMetadata()
  {
    try (RetryingPooledBookkeeperClient client = factory.createBookKeeperClient(conf)) {
      client.invalidateFileMetadata(remotePath);
    }
    catch (Exception e) {
      log.error("Could not Invalidate Corrupted File " + remotePath + " Error : ", e);
    }
  }

  private long readFromRemoteFileSystem() throws IOException
  {
    // Setting the cached read data to zero as we are reading the whole request from remote object store
    read = 0;

    if (cancelled) {
      return 0;
    }

    try (FSDataInputStream inputStream = remoteFileSystem.open(new Path(remotePath))) {
      directReadChain = new DirectReadRequestChain(inputStream);
      for (ReadRequest readRequest : readRequests) {
        directReadChain.addReadRequest(readRequest);
      }
      directReadChain.lock();
      long directRead = directReadChain.call();
      directReadChain = null;
      return directRead;
    }
  }

  public ReadRequestChainStats getStats()
  {
    return new ReadRequestChainStats()
        .setDirectDataRead(directDataRead)
        .setCachedDataRead(read)
        .setCachedReads(requests)
        .setCorruptedFileCount(corruptedFileCount);
  }
}
