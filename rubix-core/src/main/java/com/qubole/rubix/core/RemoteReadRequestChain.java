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
import com.qubole.rubix.spi.CacheConfig;
import com.qubole.rubix.spi.CacheUtil;
import com.qubole.rubix.spi.RetryingPooledBookkeeperClient;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.util.DirectBufferPool;

import java.io.EOFException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static com.google.common.base.Preconditions.checkState;

/**
 * Created by stagra on 4/1/16.
 * <p>
 * This chain reads from Remote and stores one copy in cache
 */
public class RemoteReadRequestChain extends ReadRequestChain
{
  final FSDataInputStream inputStream;

  private DirectBufferPool bufferPool;
  private int directBufferSize;
  private byte[] affixBuffer;
  private long extraRead;
  private long totalRequestedRead;
  private long warmupPenalty;
  private int blockSize;

  private BookKeeperFactory bookKeeperFactory;

  private static final Log log = LogFactory.getLog(RemoteReadRequestChain.class);

  private String localFile;

  public RemoteReadRequestChain(FSDataInputStream inputStream,
      String remotePath,
      int generationNumber,
      DirectBufferPool bufferPool,
      Configuration conf,
      byte[] affixBuffer,
      BookKeeperFactory bookKeeperFactory)
  {
    super(generationNumber);
    this.inputStream = inputStream;
    this.bufferPool = bufferPool;
    this.directBufferSize = CacheConfig.getDiskReadBufferSize(conf);
    this.affixBuffer = affixBuffer;
    this.blockSize = affixBuffer.length;
    this.localFile = CacheUtil.getLocalPath(remotePath, conf, generationNumber);
    this.bookKeeperFactory = bookKeeperFactory;
  }

  @VisibleForTesting
  public RemoteReadRequestChain(FSDataInputStream inputStream, String remoteFileName, int generationNumber, Configuration conf)
  {
    this(inputStream, remoteFileName, generationNumber, new DirectBufferPool(), conf, new byte[100], new BookKeeperFactory());
  }

  public Long call()
      throws IOException
  {
    log.debug(String.format("Read Request threadName: %s, Remote read Executor threadName: %s", threadName, Thread.currentThread().getName()));
    Thread.currentThread().setName(threadName);
    checkState(isLocked, "Trying to execute Chain without locking");

    if (readRequests.size() == 0) {
      return 0L;
    }

    // Issue-53 : Open file with the right permissions
    File file = new File(localFile);
    if (!file.exists()) {
      throw new IOException(String.format("File does not exists %s", localFile));
    }

    FileChannel fileChannel = new FileOutputStream(new RandomAccessFile(file, "rw").getFD()).getChannel();
    ByteBuffer directBuffer = bufferPool.getBuffer(directBufferSize);
    checkState(directBuffer != null, "directBuffer could not be allocated");
    try {
      for (ReadRequest readRequest : readRequests) {
        if (cancelled) {
          propagateCancel(this.getClass().getName());
        }
        log.debug(String.format("Executing ReadRequest: [%d, %d, %d, %d, %d]", readRequest.getBackendReadStart(), readRequest.getBackendReadEnd(), readRequest.getActualReadStart(), readRequest.getActualReadEnd(), readRequest.getDestBufferOffset()));
        int prefixBufferLength = (int) (readRequest.getActualReadStart() - readRequest.getBackendReadStart());
        int suffixBufferLength = (int) (readRequest.getBackendReadEnd() - readRequest.getActualReadEnd());
        log.debug(String.format("PrefixLength: %d SuffixLength: %d", prefixBufferLength, suffixBufferLength));

        // Possible only for first readRequest in chain
        if (prefixBufferLength > 0) {
          // Streaming read because next read is guaranteed to be contiguous
          inputStream.seek(readRequest.backendReadStart);
          log.debug(String.format("Trying to Read %d bytes into prefix buffer", prefixBufferLength));
          extraRead += readIntoBuffer(affixBuffer, 0, prefixBufferLength, inputStream);
          int written = copyIntoCache(fileChannel, directBuffer, affixBuffer, 0, prefixBufferLength, readRequest.backendReadStart);
          log.debug(String.format("Copied %d prefix bytes into cache", written));
        }

        log.debug(String.format("Trying to Read %d bytes into destination buffer", readRequest.getActualReadLengthIntUnsafe()));
        int readBytes;
        if (prefixBufferLength > 0 || suffixBufferLength > 0) {
          // For single readRequest in chain, prefix and suffix both may be present
          // seek needed in case of just suffix being present, otherwise it is no-op
          inputStream.seek(readRequest.actualReadStart);
          readBytes = readIntoBuffer(readRequest.getDestBuffer(), readRequest.destBufferOffset, readRequest.getActualReadLengthIntUnsafe(), inputStream);
        }
        else {
          // Positioned read for all reads between first and last readRequest as these are at least rubix blockSize apart (default 1MB)
          inputStream.readFully(readRequest.actualReadStart, readRequest.getDestBuffer(), readRequest.destBufferOffset, readRequest.getActualReadLengthIntUnsafe());
          readBytes = readRequest.getActualReadLengthIntUnsafe();
        }
        int written = copyIntoCache(fileChannel, directBuffer, readRequest.destBuffer, readRequest.destBufferOffset, readRequest.getActualReadLengthIntUnsafe(), readRequest.actualReadStart);
        log.debug(String.format("Copied %d requested bytes into cache", written));
        totalRequestedRead += readRequest.getActualReadLengthIntUnsafe();

        // Possible only for last readRequest in chain
        if (suffixBufferLength > 0) {
          log.debug(String.format("Trying to Read %d bytes into suffix buffer", suffixBufferLength));
          // When we reach here it should be in continuation of a streaming read, seek just to be safe
          inputStream.seek(readRequest.actualReadEnd);
          extraRead += readIntoBuffer(affixBuffer, 0, suffixBufferLength, inputStream);
          written = copyIntoCache(fileChannel, directBuffer, affixBuffer, 0, suffixBufferLength, readRequest.actualReadEnd);
          log.debug(String.format("Copied %d suffix bytes into cache", written));
        }
      }
      log.debug(String.format("Read %d bytes from remote localFile, added %d to destination buffer", extraRead + totalRequestedRead, totalRequestedRead));
      return totalRequestedRead;
    }
    finally {
      bufferPool.returnBuffer(directBuffer);
      fileChannel.close();
    }
  }

  public static int readIntoBuffer(byte[] destBuffer, int destBufferOffset, int length, FSDataInputStream inputStream)
      throws IOException
  {
    int nread = 0;
    while (nread < length) {
      int nbytes = inputStream.read(destBuffer, destBufferOffset + nread, length - nread);
      if (nbytes < 0) {
        throw new EOFException("End of file reached before reading fully.");
      }
      nread += nbytes;
    }
    return nread;
  }

  private int copyIntoCache(FileChannel fileChannel, ByteBuffer directBuffer, byte[] destBuffer, int destBufferOffset, int length, long cacheReadStart)
      throws IOException
  {
    log.debug(String.format("Trying to copy [%d - %d] bytes into cache with offset %d into localFile %s", cacheReadStart, cacheReadStart + length, destBufferOffset, localFile));
    long start = System.nanoTime();
    int leftToWrite = length;
    int writtenSoFar = 0;
    while (leftToWrite > 0) {
      int writeInThisCycle = Math.min(leftToWrite, directBuffer.capacity());
      directBuffer.clear();
      directBuffer.put(destBuffer, destBufferOffset + writtenSoFar, writeInThisCycle);
      directBuffer.flip();
      int nwrite = fileChannel.write(directBuffer, cacheReadStart + writtenSoFar);
      writtenSoFar += nwrite;
      leftToWrite -= nwrite;
    }
    warmupPenalty += System.nanoTime() - start;
    return writtenSoFar;
  }

  public ReadRequestChainStats getStats()
  {
    return new ReadRequestChainStats()
            .setRemoteRRCDataRead(extraRead + totalRequestedRead)
            .setRemoteRRCExtraDataRead(extraRead)
            .setRemoteRRCWarmupTime(warmupPenalty)
            .setRemoteRRCRequests(requests);
  }

  @Override
  public void updateCacheStatus(String remotePath, long fileSize, long lastModified, int blockSize, Configuration conf)
  {
    try (RetryingPooledBookkeeperClient client = bookKeeperFactory.createBookKeeperClient(conf)) {
      for (ReadRequest readRequest : readRequests) {
        log.debug("Setting cached from : " + toBlock(readRequest.getBackendReadStart()) + " block to : " + (toBlock(readRequest.getBackendReadEnd() - 1) + 1));
        client.setAllCached(remotePath, fileSize, lastModified, toBlock(readRequest.getBackendReadStart()), toBlock(readRequest.getBackendReadEnd() - 1) + 1, generationNumber);
      }
    }
    catch (Exception e) {
      log.warn("Could not update BookKeeper about newly cached blocks", e);
    }
  }

  private long toBlock(long pos)
  {
    return pos / blockSize;
  }
}
