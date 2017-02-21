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
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.qubole.rubix.spi.BlockLocation;
import com.qubole.rubix.spi.BookKeeperFactory;
import com.qubole.rubix.spi.CacheConfig;
import com.qubole.rubix.spi.ClusterType;
import com.qubole.rubix.spi.Location;
import com.qubole.rubix.spi.RetryingBookkeeperClient;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.SoftReferenceObjectPool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.DirectBufferPool;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * Created by stagra on 29/12/15.
 */
public class CachingInputStream
        extends FSInputStream
{
    private FSDataInputStream inputStream;

    private long nextReadPosition;
    private long nextReadBlock;
    private int blockSize;
    private RandomAccessFile localFileForReading = null;
    private RandomAccessFile localFileForWriting = null;
    private CachingFileSystemStats statsMbean;

    private static ListeningExecutorService readService = MoreExecutors.listeningDecorator(Executors.newCachedThreadPool());
    private static final Log log = LogFactory.getLog(CachingInputStream.class);

    private String remotePath;
    private long fileSize;
    private String localPath;
    private long lastModified;

    private RetryingBookkeeperClient bookKeeperClient;
    private Configuration conf;

    private boolean strictMode = false;
    ClusterType clusterType;
    FileSystem remoteFileSystem;

    private static DirectBufferPool bufferPool = new DirectBufferPool();
    private ByteBuffer directWriteBuffer = null;
    private ByteBuffer directReadBuffer = null;
    private byte[] affixBuffer;
    private int diskReadBufferSize;

    private static ObjectPool<ReadRequest> readRequestPool = new SoftReferenceObjectPool<>(new ReadRequestFactory());

    public CachingInputStream(FSDataInputStream parentInputStream, FileSystem parentFs, Path backendPath, Configuration conf, CachingFileSystemStats statsMbean, ClusterType clusterType, BookKeeperFactory bookKeeperFactory, FileSystem remoteFileSystem)
            throws IOException
    {
        this.remotePath = backendPath.toString();
        FileStatus fileStatus = parentFs.getFileStatus(backendPath);
        this.fileSize = fileStatus.getLen();
        this.remoteFileSystem = remoteFileSystem;
        lastModified = fileStatus.getModificationTime();
        initialize(parentInputStream,
                conf, bookKeeperFactory);
        this.statsMbean = statsMbean;
        this.clusterType = clusterType;
    }

    @VisibleForTesting
    public CachingInputStream(FSDataInputStream parentInputStream, Configuration conf, Path backendPath, long size, long lastModified, CachingFileSystemStats statsMbean, ClusterType clusterType, BookKeeperFactory bookKeeperFactory, FileSystem remoteFileSystem)
            throws IOException
    {
        this.remotePath = backendPath.toString();
        this.fileSize = size;
        this.lastModified = lastModified;
        initialize(parentInputStream, conf, bookKeeperFactory);
        this.statsMbean = statsMbean;
        this.clusterType = clusterType;
    }

    private void initialize(FSDataInputStream parentInputStream, Configuration conf, BookKeeperFactory bookKeeperFactory)
    {
        this.conf = conf;
        this.strictMode = CacheConfig.isStrictMode(conf);
        try {
            this.bookKeeperClient = bookKeeperFactory.createBookKeeperClient(conf);
        }
        catch (Exception e) {
            if (strictMode) {
                throw Throwables.propagate(e);
            }
            log.warn("Could not create BookKeeper Client " + Throwables.getStackTraceAsString(e));
            bookKeeperClient = null;
        }
        this.inputStream = checkNotNull(parentInputStream, "ParentInputStream is null");
        this.blockSize = CacheConfig.getBlockSize(conf);
        this.localPath = CacheConfig.getLocalPath(remotePath, conf);
        this.diskReadBufferSize = CacheConfig.getDiskReadBufferSizeDefault(conf);
        File file = new File(localPath);
        if (!file.exists()) {
            try {
                file.createNewFile();
            }
            catch (IOException e) {
                log.error("Error in creating local file " + localPath, e);
                // reset bookkeeper client so that we take direct route
                this.bookKeeperClient = null;
            }
            file.setReadable(true, false);
            file.setWritable(true, false);
        }
    }

    @Override
    public void seek(long pos)
            throws IOException
    {
        checkState(pos >= 0, "Negative Position");
        log.debug(String.format("Seek request, currentPos: %d currentBlock: %d", nextReadPosition, nextReadBlock));
        this.nextReadPosition = pos;
        setNextReadBlock();
        log.debug(String.format("Seek to %d, setting block location %d", nextReadPosition, nextReadBlock));
    }

    @Override
    public long getPos()
            throws IOException
    {
        return nextReadPosition;
    }

    @Override
    public boolean seekToNewSource(long l)
            throws IOException
    {
        return false;
    }

    @Override
    public int read()
            throws IOException
    {
        // This stream is wrapped with BufferedInputStream, so this method should never be called
        throw new UnsupportedOperationException();
    }

    @Override
    public int read(byte[] buffer, int offset, int length)
            throws IOException
    {
        log.debug(String.format("Got Read, currentPos: %d currentBlock: %d bufferOffset: %d length: %d", nextReadPosition, nextReadBlock, offset, length));
        if (nextReadPosition >= fileSize) {
            log.debug("Already at eof, returning");
            return -1;
        }

        // Get the last block
        final long endBlock = ((nextReadPosition + (length - 1)) / blockSize) + 1; // this block will not be read

        log.debug(String.format("Before allocation idle %d, active %d", readRequestPool.getNumIdle(), readRequestPool.getNumActive()));
        // Create read requests
        final List<ReadRequestChain> readRequestChains = setupReadRequestChains(buffer,
                offset,
                endBlock,
                length);

        log.debug(String.format("After allocation idle %d, active %d", readRequestPool.getNumIdle(), readRequestPool.getNumActive()));
        log.debug("Executing Chains");

        // start read requests
        ImmutableList.Builder builder = ImmutableList.builder();
        int sizeRead = 0;

        for (ReadRequestChain readRequestChain : readRequestChains) {
            readRequestChain.lock();
            builder.add(readService.submit(readRequestChain));
        }

        List<ListenableFuture<Integer>> futures = builder.build();
        try {
            for (ListenableFuture<Integer> future : futures) {
                sizeRead += future.get();
            }
        }
        catch (InterruptedException e) {
            throw Throwables.propagate(e);
        }
        catch (ExecutionException e) {
            throw Throwables.propagate(e);
        }

        // mark all read blocks cached
        // We can let this is happen in background
        final long lastBlock = nextReadBlock;
        readService.execute(new Runnable()
        {
            @Override
            public void run()
            {
                ReadRequestChainStats stats = new ReadRequestChainStats();
                for (ReadRequestChain readRequestChain : readRequestChains) {
                    readRequestChain.updateCacheStatus(remotePath, fileSize, lastModified, blockSize, conf);
                    stats = stats.add(readRequestChain.getStats());
                }
                statsMbean.addReadRequestChainStats(stats);
            }
        });

        log.debug(String.format("Read %d bytes", sizeRead));
        if (sizeRead > 0) {
            nextReadPosition += sizeRead;
            setNextReadBlock();
            log.debug(String.format("New nextReadPosition: %d nextReadBlock: %d", nextReadPosition, nextReadBlock));
        }
        return sizeRead;
    }

    private List<ReadRequestChain> setupReadRequestChains(byte[] buffer,
                                                          int offset,
                                                          long endBlock,
                                                          int length)
    {
        DirectReadRequestChain directReadRequestChain = null;
        RemoteReadRequestChain remoteReadRequestChain = null;
        CachedReadRequestChain cachedReadRequestChain = null;
        Map<String, NonLocalReadRequestChain> nonLocalRequests = new HashMap<>();
        ImmutableList.Builder chainedReadRequestChainBuilder = ImmutableList.builder();

        int lengthAlreadyConsidered = 0;
        List<BlockLocation> isCached = null;

        try {
            if (bookKeeperClient != null) {
                isCached = bookKeeperClient.getCacheStatus(remotePath, fileSize, lastModified, nextReadBlock, endBlock, clusterType.ordinal());
            }
        }
        catch (Exception e) {
            if (strictMode) {
                throw Throwables.propagate(e);
            }
            log.info("Could not get cache status from server " + Throwables.getStackTraceAsString(e));
        }

        int idx = 0;
        for (long blockNum = nextReadBlock; blockNum < endBlock; blockNum++, idx++) {
            long backendReadStart = blockNum * blockSize;
            long backendReadEnd = (blockNum + 1) * blockSize;

            // if backendReadStart is after EOF, then return. It can happen while reading last block and enf of read covers multiple blocks after EOF
            if (backendReadStart >= fileSize) {
                log.debug("Reached EOF, returning");
                break;
            }
            if (backendReadEnd >= fileSize) {
                backendReadEnd = fileSize;
            }
            long actualReadStart = (blockNum == nextReadBlock ? nextReadPosition : backendReadStart);
            long actualReadEnd = (blockNum == (endBlock - 1) ? (nextReadPosition + length) : backendReadEnd);
            if (actualReadEnd >= fileSize) {
                actualReadEnd = fileSize;
            }
            int bufferOffest = offset + lengthAlreadyConsidered;

            ReadRequest readRequest = null;
            try {
                 readRequest = getReadRequestFromPool(backendReadStart,
                        backendReadEnd,
                        actualReadStart,
                        actualReadEnd,
                        buffer,
                        bufferOffest,
                        fileSize);
            }
            catch (Exception e) {
                //This should never occur
                log.error("Unable to create readrequest", e);
            }
            lengthAlreadyConsidered += readRequest.getActualReadLength();

            if (isCached == null) {
                log.debug(String.format("Sending block %d to DirectReadRequestChain", blockNum));
                if (directReadRequestChain == null) {
                    directReadRequestChain = new DirectReadRequestChain(inputStream, readRequestPool);
                }
                directReadRequestChain.addReadRequest(readRequest);
            }

            else if (isCached.get(idx).getLocation() == Location.CACHED) {
                log.debug(String.format("Sending cached block %d to cachedReadRequestChain", blockNum));
                try {
                    if (localFileForReading == null) {
                        this.localFileForReading = new RandomAccessFile(localPath, "r");
                    }
                    if (directReadBuffer == null) {
                        directReadBuffer = bufferPool.getBuffer(diskReadBufferSize);
                    }
                    if (cachedReadRequestChain == null) {
                        cachedReadRequestChain = new CachedReadRequestChain(localFileForReading, readRequestPool, directReadBuffer);
                    }
                }
                catch (IOException e) {
                    log.error("Unable to open file channel in R mode", e);
                    // reset bookkeeper client so that we take direct route
                    this.bookKeeperClient = null;
                    isCached = null;
                    idx--;
                    blockNum--;
                }
                cachedReadRequestChain.addReadRequest(readRequest);
            }
            else {
                if (isCached.get(idx).getLocation() == Location.NON_LOCAL) {
                    String remoteLocation = isCached.get(idx).getRemoteLocation();
                    log.debug(String.format("Sending block %d to NonLocalReadRequestChain to node : %s", blockNum, remoteLocation));
                    if (!nonLocalRequests.containsKey(remoteLocation)) {
                        NonLocalReadRequestChain nonLocalReadRequestChain = new NonLocalReadRequestChain(remoteLocation, fileSize, lastModified, conf, remoteFileSystem, remotePath, clusterType.ordinal(), strictMode);
                        nonLocalRequests.put(remoteLocation, nonLocalReadRequestChain);
                    }
                    nonLocalRequests.get(remoteLocation).addReadRequest(readRequest);
                }
                else {
                    log.debug(String.format("Sending block %d to remoteReadRequestChain", blockNum));
                    try {
                        if (localFileForWriting == null) {
                            this.localFileForWriting = new RandomAccessFile(localPath, "rw");
                        }
                        if (directWriteBuffer == null) {
                            directWriteBuffer = bufferPool.getBuffer(diskReadBufferSize);
                        }
                        if (affixBuffer == null) {
                            affixBuffer = new byte[blockSize];
                        }
                        if (remoteReadRequestChain == null) {
                            remoteReadRequestChain = new RemoteReadRequestChain(inputStream, localFileForWriting, readRequestPool, directWriteBuffer, affixBuffer);
                        }
                    }
                    catch (IOException e) {
                        log.error("Unable to obtain open file channel in RW mode", e);
                        // reset bookkeeper client so that we take direct route
                        this.bookKeeperClient = null;
                        isCached = null;
                        idx--;
                        blockNum--;
                    }
                    remoteReadRequestChain.addReadRequest(readRequest);
                }
            }
        }

        if (cachedReadRequestChain != null) {
            chainedReadRequestChainBuilder.add(cachedReadRequestChain);
        }

        if (directReadRequestChain != null ||
                remoteReadRequestChain != null) {
            ChainedReadRequestChain shared = new ChainedReadRequestChain();
            if (remoteReadRequestChain != null) {
                shared.addReadRequestChain(remoteReadRequestChain);
            }

            if (directReadRequestChain != null) {
                shared.addReadRequestChain(directReadRequestChain);
            }
            chainedReadRequestChainBuilder.add(shared);
        }

        if (!nonLocalRequests.isEmpty()) {
            for (NonLocalReadRequestChain nonLocalReadRequestChain1 : nonLocalRequests.values()) {
                chainedReadRequestChainBuilder.add(nonLocalReadRequestChain1);
            }
        }
        return chainedReadRequestChainBuilder.build();
    }

    private ReadRequest getReadRequestFromPool(long backendReadStart, long backendReadEnd, long actualReadStart, long actualReadEnd, byte[] buffer, int bufferOffest, long fileSize)
                throws Exception
    {
        ReadRequest readRequest = readRequestPool.borrowObject();
        readRequest.setActualReadStart(actualReadStart);
        readRequest.setActualReadEnd(actualReadEnd);
        readRequest.setBackendReadStart(backendReadStart);
        readRequest.setBackendReadEnd(backendReadEnd);
        readRequest.setDestBuffer(buffer);
        readRequest.setDestBufferOffset(bufferOffest);
        readRequest.setBackendFileSize(fileSize);
        return readRequest;
    }

    private void setNextReadBlock()
    {
        this.nextReadBlock = this.nextReadPosition / blockSize;
    }

    @Override
    public void close()
    {
        try {
            inputStream.close();
            if (localFileForReading != null) {
                localFileForReading.close();
            }
            if (localFileForWriting != null) {
                localFileForWriting.close();
            }
            if (bookKeeperClient != null) {
                bookKeeperClient.close();
            }
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }
}
