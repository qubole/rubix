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
import com.google.common.base.Throwables;
import com.qubole.rubix.spi.BookKeeperFactory;
import com.qubole.rubix.spi.CacheConfig;
import com.qubole.rubix.spi.RetryingBookkeeperClient;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.io.File;

import static com.google.common.base.Preconditions.checkState;

/**
 * Created by stagra on 4/1/16.
 *
 * This chain reads from Remote and stores one copy in cache
 */
public class RemoteReadRequestChain
        extends ReadRequestChain
{
    final FSDataInputStream inputStream;

    private ByteBuffer directBuffer;
    private byte[] affixBuffer;
    private int totalPrefixRead = 0;
    private int totalSuffixRead = 0;
    private int totalRequestedRead = 0;
    private long warmupPenalty = 0;
    private int blockSize = 0;
    private Configuration conf;

    private BookKeeperFactory bookKeeperFactory;

    private static final Log log = LogFactory.getLog(RemoteReadRequestChain.class);

    private String localFile;

    public RemoteReadRequestChain(FSDataInputStream inputStream, String localfile, ByteBuffer directBuffer,
                                  byte[] affixBuffer, Configuration conf) throws IOException
    {
        this(inputStream,
                localfile,
                directBuffer,
                affixBuffer,
                new BookKeeperFactory(),
                conf);
    }

    public RemoteReadRequestChain(FSDataInputStream inputStream, String localfile, ByteBuffer directBuffer,
                                  byte[] affixBuffer, BookKeeperFactory bookKeeperFactory,
                                  Configuration conf) throws IOException
    {
        this.inputStream = inputStream;
        this.directBuffer = directBuffer;
        this.affixBuffer = affixBuffer;
        this.blockSize = affixBuffer.length;
        this.localFile = localfile;
        this.bookKeeperFactory = bookKeeperFactory;
        this.conf = conf;
    }

    @VisibleForTesting
    public RemoteReadRequestChain(FSDataInputStream inputStream, String fileName, Configuration conf)
            throws IOException
    {
        this(inputStream, fileName, ByteBuffer.allocate(100), new byte[100], conf);
    }

    public Integer call()
            throws IOException
    {
        Thread.currentThread().setName(threadName);
        checkState(isLocked, "Trying to execute Chain without locking");

        if (readRequests.size() == 0) {
            return 0;
        }

        // Issue-53 : Open file with the right permissions
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
                log.debug(String.format("Executing ReadRequest: [%d, %d, %d, %d, %d]", readRequest.getBackendReadStart(), readRequest.getBackendReadEnd(), readRequest.getActualReadStart(), readRequest.getActualReadEnd(), readRequest.getDestBufferOffset()));
                inputStream.seek(readRequest.backendReadStart);

                int prefixBufferLength = (int) (readRequest.getActualReadStart() - readRequest.getBackendReadStart());
                int suffixBufferLength = (int) (readRequest.getBackendReadEnd() - readRequest.getActualReadEnd());
                log.debug(String.format("PrefixLength: %d SuffixLength: %d", prefixBufferLength, suffixBufferLength));

                if (prefixBufferLength > 0) {
                    log.debug(String.format("Trying to Read %d bytes into prefix buffer", prefixBufferLength));
                    totalPrefixRead += readIntoBuffer(affixBuffer, 0, prefixBufferLength);
                    log.debug(String.format("Read %d bytes into prefix buffer", prefixBufferLength));
                    copyIntoCache(fileChannel, affixBuffer, 0, prefixBufferLength, readRequest.backendReadStart);
                    log.debug(String.format("Copied %d prefix bytes into cache", prefixBufferLength));
                }

                log.debug(String.format("Trying to Read %d bytes into destination buffer", readRequest.getActualReadLength()));
                int readBytes = readIntoBuffer(readRequest.getDestBuffer(), readRequest.destBufferOffset, readRequest.getActualReadLength());
                log.debug(String.format("Read %d bytes into destination buffer", readBytes));
                copyIntoCache(fileChannel, readRequest.destBuffer, readRequest.destBufferOffset, readBytes, readRequest.actualReadStart);
                log.debug(String.format("Copied %d requested bytes into cache", readBytes));
                totalRequestedRead += readBytes;

                if (suffixBufferLength > 0) {
                    // If already in reading actually required data we get a eof, then there should not have been a suffix request
                    checkState(readBytes == readRequest.getActualReadLength(), "Actual read less than required, still requested for suffix");
                    log.debug(String.format("Trying to Read %d bytes into suffix buffer", suffixBufferLength));
                    totalSuffixRead += readIntoBuffer(affixBuffer, 0, suffixBufferLength);
                    log.debug(String.format("Read %d bytes into suffix buffer", suffixBufferLength));
                    copyIntoCache(fileChannel, affixBuffer, 0, suffixBufferLength, readRequest.actualReadEnd);
                    log.debug(String.format("Copied %d suffix bytes into cache", suffixBufferLength));
                }
            }
            log.info(String.format("Read %d bytes from remote localFile, added %d to destination buffer", totalPrefixRead + totalRequestedRead + totalSuffixRead, totalRequestedRead));
            return totalRequestedRead;
        }
        finally {
            fileChannel.close();
        }
    }

    private int readIntoBuffer(byte[] destBuffer, int destBufferOffset, int length)
            throws IOException
    {
        int nread = 0;
        while (nread < length) {
            int nbytes = inputStream.read(destBuffer, destBufferOffset + nread, length - nread);
            if (nbytes < 0) {
                break;
            }
            nread += nbytes;
        }
        return nread;
    }

    private int copyIntoCache(FileChannel fileChannel, byte[] destBuffer, int destBufferOffset, int length, long cacheReadStart)
            throws IOException
    {
        long cachedFileOffset = cacheReadStart % CacheConfig.getSplitSize(conf);
        log.info(String.format("Trying to copy [%d - %d] bytes into cache from destination buffer offset %d into " +
                "localFile %s with offset %d", cacheReadStart, cacheReadStart + length, destBufferOffset, localFile,
                cachedFileOffset));
        long start = System.nanoTime();
        int leftToWrite = length;
        int writtenSoFar = 0;
        while (leftToWrite > 0) {
            int writeInThisCycle = Math.min(leftToWrite, directBuffer.capacity());
            directBuffer.clear();
            directBuffer.put(destBuffer, destBufferOffset + writtenSoFar, writeInThisCycle);
            directBuffer.flip();
            int nwrite = fileChannel.write(directBuffer, cachedFileOffset + writtenSoFar);
            writtenSoFar += nwrite;
            leftToWrite -= nwrite;
        }
        warmupPenalty += System.nanoTime() - start;
        return writtenSoFar;
    }

    public ReadRequestChainStats getStats()
    {
        return new ReadRequestChainStats()
                .setPrefixRead(totalPrefixRead)
                .setRequestedRead(totalRequestedRead)
                .setSuffixRead(totalSuffixRead)
                .setWarmupPenalty(warmupPenalty)
                .setRemoteReads(requests);
    }

    @Override
    public void updateCacheStatus(String remotePath, long fileSize, long lastModified, int blockSize, Configuration conf)
    {
        try {
            RetryingBookkeeperClient client = bookKeeperFactory.createBookKeeperClient(conf);
            for (ReadRequest readRequest : readRequests) {
                client.setAllCached(remotePath, fileSize, lastModified, toBlock(readRequest.getBackendReadStart()), toBlock(readRequest.getBackendReadEnd() - 1) + 1);
            }
            client.close();
        }
        catch (Exception e) {
            log.info("Could not update BookKeeper about newly cached blocks: " + Throwables.getStackTraceAsString(e));
        }
    }

    private long toBlock(long pos)
    {
        return pos / blockSize;
    }
}
