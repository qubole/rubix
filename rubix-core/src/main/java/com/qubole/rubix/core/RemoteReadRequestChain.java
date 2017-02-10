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
    final FileChannel fileChannel;

    private ByteBuffer directBuffer;
    private int totalPrefixRead = 0;
    private int totalSuffixRead = 0;
    private int totalRequestedRead = 0;
    private long warmupPenalty = 0;

    private static final Log log = LogFactory.getLog(RemoteReadRequestChain.class);

    public RemoteReadRequestChain(FSDataInputStream inputStream, RandomAccessFile localFile, ByteBuffer directBuffer)
    {
        this.inputStream = inputStream;
        try {
            this.fileChannel = new FileOutputStream(localFile.getFD()).getChannel();
        }
        catch (IOException e) {
            log.error("Unable to open File Channel", e);
            throw Throwables.propagate(e);
        }
        this.directBuffer = directBuffer;
    }

    @VisibleForTesting
    public RemoteReadRequestChain(FSDataInputStream inputStream, RandomAccessFile randomAccessFile)
    {
        this(inputStream, randomAccessFile, ByteBuffer.allocate(100));
    }

    public Integer call()
            throws IOException
    {
        Thread.currentThread().setName(threadName);
        checkState(isLocked, "Trying to execute Chain without locking");

        if (readRequests.size() == 0) {
            return 0;
        }

        for (ReadRequest readRequest : readRequests) {
            log.debug(String.format("Executing ReadRequest: [%d, %d, %d, %d, %d]", readRequest.getBackendReadStart(), readRequest.getBackendReadEnd(), readRequest.getActualReadStart(), readRequest.getActualReadEnd(), readRequest.getDestBufferOffset()));
            inputStream.seek(readRequest.actualReadStart);
            log.debug(String.format("Trying to Read %d bytes into destination buffer", readRequest.getActualReadLength()));
            //Avoid writing partial blocks into cache.
            boolean writeIntoCache = !isPrefixOrSuffixBlock(readRequest);
            int readBytes = readAndCopy(readRequest.getDestBuffer(), readRequest.destBufferOffset, readRequest.getActualReadLength(), readRequest.getActualReadStart(), writeIntoCache);
            totalRequestedRead += readBytes;
        }
        return totalRequestedRead;
    }

    private int readAndCopy(byte[] destBuffer, int destBufferOffset, int length, long actualReadStart, boolean writeIntoCache)
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
        log.debug(String.format("Read %d bytes into destination buffer", nread));
        if (writeIntoCache) {
//            assertTrue("Wrong amount of bytes read from inputStream. Should've read the whole block", nread == length);
            long start = System.nanoTime();
            int leftToRead = nread;
            int readSoFar = 0;
            while (leftToRead > 0) {
                int readInThisCycle = Math.min(leftToRead, directBuffer.capacity());
                directBuffer.position(0);
                directBuffer.put(destBuffer, destBufferOffset + readSoFar, readInThisCycle);
                directBuffer.flip();
                int writtenInThisCycle = fileChannel.write(directBuffer, actualReadStart + readSoFar);
                readSoFar += writtenInThisCycle;
                leftToRead -= writtenInThisCycle;
            }
            log.debug(String.format("Cached data from %d to %d", actualReadStart, actualReadStart + readSoFar));
            warmupPenalty += System.nanoTime() - start;
        }
        return nread;
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
            BookKeeperFactory bookKeeperFactory = new BookKeeperFactory();
            RetryingBookkeeperClient client = bookKeeperFactory.createBookKeeperClient(conf);
            for (ReadRequest readRequest : readRequests) {
                if (!isPrefixOrSuffixBlock(readRequest)) {
                    client.setAllCached(remotePath, fileSize, lastModified, toBlock(readRequest.getBackendReadStart(), blockSize), toBlock(readRequest.getBackendReadEnd() - 1, blockSize) + 1);
                }
            }
            client.close();
        }
        catch (Exception e) {
            log.info("Could not update BookKeeper about newly cached blocks: " + Throwables.getStackTraceAsString(e));
        }
    }

    private boolean isPrefixOrSuffixBlock(ReadRequest readRequest)
    {
        return (readRequest.actualReadStart != readRequest.backendReadStart) || (readRequest.actualReadEnd != readRequest.backendReadEnd);
    }

    private long toBlock(long pos, int blockSize)
    {
        return pos / blockSize;
    }
}
