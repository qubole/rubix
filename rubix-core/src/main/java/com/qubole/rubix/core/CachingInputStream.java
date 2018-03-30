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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.DirectBufferPool;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

import static com.google.common.base.Preconditions.checkState;

/**
 * Created by stagra on 29/12/15.
 */
public class CachingInputStream extends FSInputStream
{
  private FSDataInputStream inputStream;

  private long nextReadPosition;
  private long nextReadBlock;
  private int blockSize;
  private CachingFileSystemStats statsMbean;

  private static ListeningExecutorService readService = MoreExecutors.listeningDecorator(Executors.newCachedThreadPool());
  private static final Log log = LogFactory.getLog(CachingInputStream.class);

  private String remotePath;
  private long fileSize;
  private String localPath;
  private long lastModified;

  private RetryingBookkeeperClient bookKeeperClient;
  private Configuration conf;

  private boolean strictMode;
  ClusterType clusterType;
  FileSystem remoteFileSystem;
  FileSystem.Statistics statistics;

  private static DirectBufferPool bufferPool = new DirectBufferPool();
  private ByteBuffer directWriteBuffer;
  private ByteBuffer directReadBuffer;
  private byte[] affixBuffer;
  private int diskReadBufferSize;
  private int bufferSize;

  public CachingInputStream(FileSystem parentFs, Path backendPath, Configuration conf,
                            CachingFileSystemStats statsMbean, ClusterType clusterType,
                            BookKeeperFactory bookKeeperFactory, FileSystem remoteFileSystem,
                            int bufferSize, FileSystem.Statistics statistics) throws IOException
  {
    this.remotePath = backendPath.toString();
    FileStatus fileStatus = parentFs.getFileStatus(backendPath);
    this.fileSize = fileStatus.getLen();
    this.remoteFileSystem = remoteFileSystem;
    lastModified = fileStatus.getModificationTime();
    initialize(conf, bookKeeperFactory);
    this.statsMbean = statsMbean;
    this.clusterType = clusterType;
    this.bufferSize = bufferSize;
    this.statistics = statistics;
  }

  @VisibleForTesting
  public CachingInputStream(FSDataInputStream inputStream, Configuration conf, Path backendPath,
                            long size, long lastModified, CachingFileSystemStats statsMbean,
                            ClusterType clusterType, BookKeeperFactory bookKeeperFactory,
                            FileSystem remoteFileSystem, int buffersize, FileSystem.Statistics statistics)
      throws IOException
  {
    this.inputStream = inputStream;
    this.remotePath = backendPath.toString();
    this.fileSize = size;
    this.lastModified = lastModified;
    initialize(conf, bookKeeperFactory);
    this.statsMbean = statsMbean;
    this.clusterType = clusterType;
    this.remoteFileSystem = remoteFileSystem;
    this.bufferSize = bufferSize;
    this.statistics = statistics;
  }

  private void initialize(Configuration conf, BookKeeperFactory bookKeeperFactory)
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
    this.blockSize = CacheConfig.getBlockSize(conf);
    this.localPath = CacheConfig.getLocalPath(remotePath, conf);
    this.diskReadBufferSize = CacheConfig.getDiskReadBufferSizeDefault(conf);
  }

  private FSDataInputStream getParentDataInputStream() throws IOException
  {
    if (inputStream == null) {
      inputStream = remoteFileSystem.open(new Path(remotePath), bufferSize);
    }
    return inputStream;
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
    try {
      return readInternal(buffer, offset, length);
    }
    catch (InterruptedException e) {
      throw Throwables.propagate(e);
    }
    catch (Exception e) {
      log.error(String.format("Failed to read from rubix for file %s position %d length %d. Falling back to remote", localPath, nextReadPosition, length), e);
      getParentDataInputStream().seek(nextReadPosition);
      int read = readFullyDirect(buffer, offset, length);
      if (read > 0) {
        nextReadPosition += read;
        setNextReadBlock();
      }
      return read;
    }
  }

  private int readFullyDirect(byte[] buffer, int offset, int length)
      throws IOException
  {
    int nread = 0;
    while (nread < length) {
      int nbytes = getParentDataInputStream().read(buffer, offset + nread, length - nread);
      if (nbytes < 0) {
        return nread;
      }
      nread += nbytes;
    }
    return nread;
  }

  private int readInternal(byte[] buffer, int offset, int length)
      throws IOException, InterruptedException, ExecutionException

  {
    log.debug(String.format("Got Read, currentPos: %d currentBlock: %d bufferOffset: %d length: %d", nextReadPosition, nextReadBlock, offset, length));

    if (nextReadPosition >= fileSize) {
      log.debug("Already at eof, returning");
      return -1;
    }

    // Get the last block
    final long endBlock = ((nextReadPosition + (length - 1)) / blockSize) + 1; // this block will not be read

    // Create read requests
    final List<ReadRequestChain> readRequestChains = setupReadRequestChains(buffer,
        offset,
        endBlock,
        length);

    log.debug("Executing Chains");

    // start read requests
    ImmutableList.Builder builder = ImmutableList.builder();
    int sizeRead = 0;

    for (ReadRequestChain readRequestChain : readRequestChains) {
      readRequestChain.lock();
      builder.add(readService.submit(readRequestChain));
    }

    List<ListenableFuture<Integer>> futures = builder.build();
    for (ListenableFuture<Integer> future : futures) {
      // exceptions handled in caller
      try {
        sizeRead += future.get();
      }
      catch (ExecutionException | InterruptedException e) {
        for (ReadRequestChain readRequestChain : readRequestChains) {
          readRequestChain.cancel();
        }
        throw e;
      }
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

    log.info(String.format("Read %d bytes", sizeRead));
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
                                                        int length) throws IOException
  {
    DirectReadRequestChain directReadRequestChain = null;
    RemoteReadRequestChain remoteReadRequestChain = null;
    CachedReadRequestChain cachedReadRequestChain = null;
    RemoteFetchRequestChain remoteFetchRequestChain = null;
    Map<String, NonLocalReadRequestChain> nonLocalRequests = new HashMap<>();
    Map<String, NonLocalRequestChain> nonLocalAsyncRequests = new HashMap<String, NonLocalRequestChain>();
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
          directReadRequestChain = new DirectReadRequestChain(getParentDataInputStream());
        }
        directReadRequestChain.addReadRequest(readRequest);
      }

      else if (isCached.get(idx).getLocation() == Location.CACHED) {
        log.debug(String.format("Sending cached block %d to cachedReadRequestChain", blockNum));
        try {
          if (directReadBuffer == null) {
            directReadBuffer = bufferPool.getBuffer(diskReadBufferSize);
          }
          if (cachedReadRequestChain == null) {
            cachedReadRequestChain = new CachedReadRequestChain(localPath, directReadBuffer, statistics);
          }
          cachedReadRequestChain.addReadRequest(readRequest);
        }
        catch (IOException e) {
          log.error("Unable to open file channel in R mode", e);
          // reset bookkeeper client so that we take direct route
          this.bookKeeperClient = null;
          isCached = null;
          idx--;
          blockNum--;
        }
      }
      else {
        if (isCached.get(idx).getLocation() == Location.NON_LOCAL) {
          String remoteLocation = isCached.get(idx).getRemoteLocation();

          if (CacheConfig.isParallelWarmupEnabled(conf)) {
            log.debug(String.format("Sending block %d to NonLocalRequestChain to node : %s", blockNum, remoteLocation));
            if (!nonLocalAsyncRequests.containsKey(remoteLocation)) {
              NonLocalRequestChain nonLocalRequestChain =
                  new NonLocalRequestChain(remoteLocation, fileSize, lastModified,
                      conf, remoteFileSystem, remotePath, clusterType.ordinal(), strictMode,
                      statistics, nextReadBlock, endBlock);
              nonLocalAsyncRequests.put(remoteLocation, nonLocalRequestChain);
            }
            nonLocalAsyncRequests.get(remoteLocation).addReadRequest(readRequest);
            if (nonLocalAsyncRequests.get(remoteLocation).needDirectReadRequest(blockNum)) {
              if (directReadRequestChain == null) {
                directReadRequestChain = new DirectReadRequestChain(getParentDataInputStream());
              }
              directReadRequestChain.addReadRequest(readRequest);
            }
          }
          else {
            log.debug(String.format("Sending block %d to NonLocalReadRequestChain to node : %s", blockNum, remoteLocation));
            if (!nonLocalRequests.containsKey(remoteLocation)) {
              NonLocalReadRequestChain nonLocalReadRequestChain =
                  new NonLocalReadRequestChain(remoteLocation, fileSize, lastModified, conf,
                      remoteFileSystem, remotePath, clusterType.ordinal(), strictMode, statistics);
              nonLocalRequests.put(remoteLocation, nonLocalReadRequestChain);
            }
            nonLocalRequests.get(remoteLocation).addReadRequest(readRequest);
          }
        }
        else {
          if (directWriteBuffer == null) {
            directWriteBuffer = bufferPool.getBuffer(diskReadBufferSize);
          }
          if (CacheConfig.isParallelWarmupEnabled(conf)) {
            log.debug(String.format("Sending block %d to remoteFetchRequestChain", blockNum));
            if (directReadRequestChain == null) {
              directReadRequestChain = new DirectReadRequestChain(getParentDataInputStream());
            }

            if (remoteFetchRequestChain == null) {
              remoteFetchRequestChain = new RemoteFetchRequestChain(remotePath, remoteFileSystem,
                  "localhost", conf, lastModified, fileSize, clusterType.ordinal());
            }

            directReadRequestChain.addReadRequest(readRequest);
            remoteFetchRequestChain.addReadRequest(readRequest.clone());
          }
          else {
            log.debug(String.format("Sending block %d to remoteReadRequestChain", blockNum));
            try {
              if (affixBuffer == null) {
                affixBuffer = new byte[blockSize];
              }
              if (remoteReadRequestChain == null) {
                remoteReadRequestChain = new RemoteReadRequestChain(getParentDataInputStream(), localPath, directWriteBuffer, affixBuffer);
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
    }

    if (cachedReadRequestChain != null) {
      chainedReadRequestChainBuilder.add(cachedReadRequestChain);
    }

    if (!CacheConfig.isParallelWarmupEnabled(conf)) {
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
    }
    else {
      if (directReadRequestChain != null) {
        if (directReadRequestChain != null) {
          chainedReadRequestChainBuilder.add(directReadRequestChain);
        }

        if (remoteFetchRequestChain != null) {
          chainedReadRequestChainBuilder.add(remoteFetchRequestChain);
        }
      }
    }

    if (!CacheConfig.isParallelWarmupEnabled(conf)) {
      if (!nonLocalRequests.isEmpty()) {
        for (NonLocalReadRequestChain nonLocalReadRequestChain1 : nonLocalRequests.values()) {
          chainedReadRequestChainBuilder.add(nonLocalReadRequestChain1);
        }
      }
    }
    else {
      if (!nonLocalAsyncRequests.isEmpty()) {
        for (NonLocalRequestChain item : nonLocalAsyncRequests.values()) {
          chainedReadRequestChainBuilder.add(item);
        }
      }
    }

    return chainedReadRequestChainBuilder.build();
  }

  private void setNextReadBlock()
  {
    this.nextReadBlock = this.nextReadPosition / blockSize;
  }

  private void returnBuffers()
  {
    if (directWriteBuffer != null) {
      bufferPool.returnBuffer(directWriteBuffer);
      directWriteBuffer = null;
    }

    if (directReadBuffer != null) {
      bufferPool.returnBuffer(directReadBuffer);
      directReadBuffer = null;
    }
  }

  @Override
  public void close()
  {
    returnBuffers();
    try {
      if (inputStream != null) {
        inputStream.close();
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
