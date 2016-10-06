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
import com.qubole.rubix.bookkeeper.BookKeeperService;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import static com.google.common.base.Preconditions.checkState;
import static com.qubole.rubix.bookkeeper.RetryingBookkeeperClient.createBookKeeperClient;

/**
 * Created by stagra on 4/1/16.
 *
 * This chain reads from Remote and stores one copy in cache
 */
public class RemoteReadRequestChain
        extends ReadRequestChain
{
    final FSDataInputStream inputStream;
    final String localFilename;

    private int totalPrefixRead = 0;
    private int totalSuffixRead = 0;
    private int totalRequestedRead = 0;
    private long warmupPenalty = 0;

    private static final Log log = LogFactory.getLog(RemoteReadRequestChain.class);

    public RemoteReadRequestChain(FSDataInputStream inputStream, String localFile)
    {
        this.inputStream = inputStream;
        this.localFilename = localFile;
    }

    public Integer call()
            throws IOException
    {
        Thread.currentThread().setName(threadName);
        checkState(isLocked, "Trying to execute Chain without locking");

        if (readRequests.size() == 0) {
            return 0;
        }

        RandomAccessFile localFile = null;
        FileChannel fc = null;

        try {
            localFile = new RandomAccessFile(localFilename, "rw");
            fc = localFile.getChannel();
            for (ReadRequest readRequest : readRequests) {
                log.debug(String.format("Executing ReadRequest: [%d, %d, %d, %d, %d]", readRequest.getBackendReadStart(), readRequest.getBackendReadEnd(), readRequest.getActualReadStart(), readRequest.getActualReadEnd(), readRequest.getDestBufferOffset()));
                inputStream.seek(readRequest.backendReadStart);
                MappedByteBuffer mbuf = fc.map(FileChannel.MapMode.READ_WRITE, readRequest.backendReadStart, readRequest.getBackendReadLength());
                log.debug(String.format("Mapped file from %d till length %d", readRequest.backendReadStart, readRequest.getBackendReadLength()));
                /*
                 * MappedByteBuffer does not provide backing byte array, so cannot write directly to it via FSDataOutputStream.read
                 * Instead, download to normal destination buffer (+offset buffer to get block boundaries) and then copy to MappedByteBuffer
                 */

                int prefixBufferLength = (int) (readRequest.getActualReadStart() - readRequest.getBackendReadStart());
                int suffixBufferLength = (int) (readRequest.getBackendReadEnd() - readRequest.getActualReadEnd());
                log.debug(String.format("PrefixLength: %d SuffixLength: %d", prefixBufferLength, suffixBufferLength));

                // TODO: use single byte buffer for all three streams
                /* TODO: also GC cost can be lowered by shared buffer pool, a small one.
                    IOUtils.copyLarge method. A single 4kB byte buffer can be used to copy whole file

                  */
                if (prefixBufferLength > 0) {
                    byte[] prefixBuffer = new byte[prefixBufferLength];
                    log.debug(String.format("Trying to Read %d bytes into prefix buffer", prefixBufferLength));
                    totalPrefixRead += readAndCopy(prefixBuffer, 0, mbuf, prefixBufferLength);
                    log.debug(String.format("Read %d bytes into prefix buffer", prefixBufferLength));
                }
                log.debug(String.format("Trying to Read %d bytes into destination buffer", readRequest.getActualReadLength()));
                int readBytes = readAndCopy(readRequest.getDestBuffer(), readRequest.destBufferOffset, mbuf, readRequest.getActualReadLength());
                totalRequestedRead += readBytes;
                log.debug(String.format("Read %d bytes into destination buffer", readBytes));
                if (suffixBufferLength > 0) {
                    // If already in reading actually required data we get a eof, then there should not have been a suffix request
                    checkState(readBytes == readRequest.getActualReadLength(), "Acutal read less than required, still requested for suffix");
                    byte[] suffixBuffer = new byte[suffixBufferLength];
                    log.debug(String.format("Trying to Read %d bytes into suffix buffer", suffixBufferLength));
                    totalSuffixRead += readAndCopy(suffixBuffer, 0, mbuf, suffixBufferLength);
                    log.debug(String.format("Read %d bytes into suffix buffer", suffixBufferLength));
                }
            }
        }
        finally {
            if (fc != null) {
                fc.close();
            }
            if (localFile != null) {
                localFile.close();
            }
        }
        log.info(String.format("Read %d bytes from remote file, added %d to destination buffer", totalPrefixRead + totalRequestedRead + totalSuffixRead, totalRequestedRead));
        return totalRequestedRead;
    }

    private int readAndCopy(byte[] destBuffer, int destBufferOffset, MappedByteBuffer localFileBuffer, int length)
            throws IOException
    {
        int nread = 0;
        while (nread < length) {
            int nbytes = inputStream.read(destBuffer, destBufferOffset + nread, length - nread);
            if (nbytes < 0) {
                return nread;
            }
            nread += nbytes;
        }
        long start = System.nanoTime();
        localFileBuffer.put(destBuffer, destBufferOffset, length);
        warmupPenalty += System.nanoTime() - start;
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
            BookKeeperService.Client client = createBookKeeperClient(conf);
            for (ReadRequest readRequest : readRequests) {
                client.setAllCached(remotePath, fileSize, lastModified, toBlock(readRequest.getBackendReadStart(), blockSize), toBlock(readRequest.getBackendReadEnd() - 1, blockSize) + 1);
            }
            client.close();
        }
        catch (Exception e) {
            log.info("Could not update BookKeeper about newly cached blocks: " + Throwables.getStackTraceAsString(e));
        }
    }

    private long toBlock(long pos, int blockSize)
    {
        return pos / blockSize;
    }
}
