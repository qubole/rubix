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
import com.qubole.rubix.bookkeeper.BookKeeperClient;
import com.qubole.rubix.bookkeeper.BookKeeperConfig;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetAddress;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.qubole.rubix.bookkeeper.BookKeeperClient.createBookKeeperClient;

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
    private CachingFileSystemStats statsMbean;

    private static ListeningExecutorService readService = MoreExecutors.listeningDecorator(Executors.newCachedThreadPool());
    private static final Log log = LogFactory.getLog(CachingInputStream.class);

    private String remotePath;
    private long fileSize;
    private String localPath;
    private long lastModified;

    private BookKeeperClient bookKeeperClient;
    private Configuration conf;

    private boolean strictMode = false;
    private boolean localityInfoPresent = false;
    private Set<String> localSplits;
    private long splitSize;

    public CachingInputStream(FSDataInputStream parentInputStream, FileSystem parentFs, Path backendPath, Configuration conf, CachingFileSystemStats statsMbean, long splitSize)
            throws IOException
    {
        this.remotePath = backendPath.toString();
        this.fileSize = parentFs.getLength(backendPath);
        lastModified = parentFs.getFileStatus(backendPath).getModificationTime();
        initialize(parentInputStream,
                conf);
        this.statsMbean = statsMbean;
        String nodename = InetAddress.getLocalHost().getCanonicalHostName();
        localSplits = CachingConfigHelper.getLocalityInfo(conf, nodename, backendPath.toString());
        this.splitSize = splitSize;
        this.localityInfoPresent = CachingConfigHelper.isLocalityInfoForwarded(conf);
    }

    @VisibleForTesting
    public CachingInputStream(FSDataInputStream parentInputStream, Configuration conf, Path backendPath, long size, long lastModified, CachingFileSystemStats statsMbean, long splitSize)
            throws IOException
    {
        this.remotePath = backendPath.toString();
        this.fileSize = size;
        this.lastModified = lastModified;
        initialize(parentInputStream, conf);
        this.statsMbean = statsMbean;
        this.splitSize = splitSize;
    }

    private void initialize(FSDataInputStream parentInputStream, Configuration conf)
            throws IOException
    {
        this.conf = conf;
        this.strictMode = CachingConfigHelper.isStrictMode(conf);
        try {
            this.bookKeeperClient = createBookKeeperClient(conf);
        }
        catch (Exception e) {
            if (strictMode) {
                throw Throwables.propagate(e);
            }
            log.warn("Could not create BookKeeper Client " + Throwables.getStackTraceAsString(e));
            bookKeeperClient = null;
        }
        this.inputStream = checkNotNull(parentInputStream, "ParentInputStream is null");
        this.blockSize = BookKeeperConfig.getBlockSize(conf);
        this.localPath = BookKeeperConfig.getLocalPath(remotePath, conf);
        try {
            this.localFileForReading = new RandomAccessFile(localPath, "r");
        }
        catch (FileNotFoundException e) {
            log.info("Creating local file " + localPath);
            File file = new File(localPath);
            file.createNewFile();
            this.localFileForReading = new RandomAccessFile(file, "rw");
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
        log.warn("YYY READ in " + InetAddress.getLocalHost().getCanonicalHostName() + ": " + conf.get("ABCD"));
        log.debug(String.format("Got Read, currentPos: %d currentBlock: %d bufferOffset: %d length: %d", nextReadPosition, nextReadBlock, offset, length));
        if (nextReadPosition >= fileSize) {
            log.debug("Already at eof, returning");
            return -1;
        }

        // Get the last block
        final long endBlock = ((nextReadPosition + (length - 1)) /  blockSize) + 1; // this block will not be read

        // Create read requests
        final List<ReadRequestChain> readRequestChains = setupReadRequestChains(buffer,
                                                        offset,
                                                        endBlock,
                                                        length);

        log.debug("Executing Chains");
        // start read requests
        ImmutableList.Builder builder = ImmutableList.builder();
        for (ReadRequestChain readRequestChain : readRequestChains) {
            readRequestChain.lock();
            builder.add(readService.submit(readRequestChain));
        }

        List<ListenableFuture<Integer>> futures = builder.build();

        int sizeRead = 0;
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
        readService.execute(new Runnable(){
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
        NonLocalReadRequestChain nonLocalReadRequestChain = null;

        ImmutableList.Builder readRequestChainBuilder = ImmutableList.builder();

        int lengthAlreadyConsidered = 0;
        List<Boolean> isCached = null;
        try {
            if (bookKeeperClient != null) {
                isCached = bookKeeperClient.getCacheStatus(remotePath, fileSize, lastModified, nextReadBlock, endBlock);
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
                return readRequestChainBuilder.build();
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

            ReadRequest readRequest = new ReadRequest(backendReadStart,
                    backendReadEnd,
                    actualReadStart,
                    actualReadEnd,
                    buffer,
                    bufferOffest,
                    fileSize);

            lengthAlreadyConsidered += readRequest.getActualReadLength();

            if (isCached == null) {
                log.debug(String.format("Sending block %d to DirectReadRequestChain", blockNum));
                if (directReadRequestChain == null) {
                    directReadRequestChain = new DirectReadRequestChain(inputStream);
                    readRequestChainBuilder.add(directReadRequestChain);
                }
                directReadRequestChain.addReadRequest(readRequest);
            }
            else if (isCached.get(idx)) {
                log.debug(String.format("Sending Cached block %d to cachedReadRequestChain", blockNum));
                if (cachedReadRequestChain == null) {
                    cachedReadRequestChain = new CachedReadRequestChain(localFileForReading);
                    readRequestChainBuilder.add(cachedReadRequestChain);
                }
                cachedReadRequestChain.addReadRequest(readRequest);

            }
            else {
                if (!isLocal(blockNum)) {
                    log.debug(String.format("Sending block %d to NonLocalReadRequestChain", blockNum));
                    if (nonLocalReadRequestChain == null) {
                        nonLocalReadRequestChain = new NonLocalReadRequestChain(inputStream);
                        readRequestChainBuilder.add(nonLocalReadRequestChain);
                    }
                    nonLocalReadRequestChain.addReadRequest(readRequest);
                }
                else {
                    log.debug(String.format("Sending block %d to remoteReadRequestChain", blockNum));
                    if (remoteReadRequestChain == null) {
                        remoteReadRequestChain = new RemoteReadRequestChain(inputStream, localPath);
                        readRequestChainBuilder.add(remoteReadRequestChain);
                    }
                    remoteReadRequestChain.addReadRequest(readRequest);
                }
            }
        }

        return readRequestChainBuilder.build();
    }

    private void setNextReadBlock()
    {
        this.nextReadBlock = this.nextReadPosition / blockSize;
    }

    private boolean isLocal(long block)
    {
        if (!localityInfoPresent) {
            return true;
        }
        long split = (block * blockSize) /  splitSize;
        return localSplits.contains(Long.toString(split));
    }

    @Override
    public void close()
    {
        try {
            inputStream.close();
            localFileForReading.close();
            if (bookKeeperClient != null) {
                bookKeeperClient.close();
            }
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }
}
