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

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.qubole.rubix.spi.BookKeeperFactory;
import com.qubole.rubix.spi.CacheConfig;
import com.qubole.rubix.spi.ClusterType;
import com.qubole.rubix.spi.RetryingPooledBookkeeperClient;
import com.qubole.rubix.spi.thrift.BlockLocation;
import com.qubole.rubix.spi.thrift.CacheStatusRequest;
import com.qubole.rubix.spi.thrift.CacheStatusResponse;
import com.qubole.rubix.spi.thrift.FileInfo;
import com.qubole.rubix.spi.thrift.Location;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.DirectBufferPool;

import javax.annotation.Nullable;

import java.io.EOFException;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import static com.qubole.rubix.spi.CacheUtil.UNKONWN_GENERATION_NUMBER;
import static org.apache.hadoop.fs.FSExceptionMessages.NEGATIVE_SEEK;

/**
 * Created by stagra on 29/12/15.
 */
public class CachingInputStream extends FSInputStream
{
  private FSDataInputStream inputStream;

  private long nextReadPosition;
  private long nextReadBlock;
  int blockSize;
  private CachingFileSystemStats statsMbean;

  static ListeningExecutorService readService = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(CacheConfig.READ_SERVICE_THREAD_POOL_SIZE, new ThreadFactory()
  {
    public Thread newThread(Runnable r)
    {
      Thread t = Executors.defaultThreadFactory().newThread(r);
      t.setName("rubix-readRequest-thread");
      t.setDaemon(true);
      return t;
    }
  }));
  private static final Log log = LogFactory.getLog(CachingInputStream.class);

  protected final String remotePath;
  private long fileSize;
  private long lastModified;

  Configuration conf;

  private boolean strictMode;
  ClusterType clusterType;
  FileSystem remoteFileSystem;
  FileSystem.Statistics statistics;

  private static DirectBufferPool bufferPool = new DirectBufferPool();
  private byte[] affixBuffer;
  private int diskReadBufferSize;
  private final int bufferSize;
  BookKeeperFactory bookKeeperFactory;

  public CachingInputStream(Path backendPath, Configuration conf,
                            CachingFileSystemStats statsMbean, ClusterType clusterType,
                            BookKeeperFactory bookKeeperFactory, FileSystem remoteFileSystem,
                            int bufferSize, FileSystem.Statistics statistics) throws IOException
  {
    this.conf = conf;
    this.strictMode = CacheConfig.isStrictMode(conf);
    this.remotePath = backendPath.toString();
    this.blockSize = CacheConfig.getBlockSize(conf);
    this.diskReadBufferSize = CacheConfig.getDiskReadBufferSize(conf);
    this.bookKeeperFactory = bookKeeperFactory;
    this.remoteFileSystem = remoteFileSystem;

    this.fileSize = -1;
    if (!CacheConfig.isFileStalenessCheckEnabled(conf)) {
      try (RetryingPooledBookkeeperClient bookKeeperClient = bookKeeperFactory.createBookKeeperClient(conf)) {
        FileInfo fileInfo = bookKeeperClient.getFileInfo(backendPath.toString());
        this.fileSize = fileInfo.fileSize;
        this.lastModified = fileInfo.lastModified;
      }
      catch (Exception ex) {
        if (strictMode) {
          throw new RuntimeException(ex);
        }
        log.error(String.format("Could not get FileInfo for %s. Fetching FileStatus from remote file system :", backendPath.toString()), ex);
      }
    }
    if (this.fileSize == -1) {
      FileStatus fileStatus = remoteFileSystem.getFileStatus(backendPath);
      this.fileSize = fileStatus.getLen();
      this.lastModified = fileStatus.getModificationTime();
    }

    this.statsMbean = statsMbean;
    this.clusterType = clusterType;
    this.bufferSize = bufferSize;
    this.statistics = statistics;
  }

  FSDataInputStream getParentDataInputStream() throws IOException
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
    if (pos < 0L) {
      throw new EOFException(NEGATIVE_SEEK);
    }
    else {
      this.nextReadPosition = pos;
      setNextReadBlock();
    }
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
      log.error(String.format("Failed to read from rubix for file %s position %d length %d. Falling back to remote", remotePath, nextReadPosition, length), e);
      getParentDataInputStream().seek(nextReadPosition);
      int read = readFullyDirect(buffer, offset, length);
      if (read > 0) {
        nextReadPosition += read;
        setNextReadBlock();
      }
      return read;
    }
  }

  protected int readFullyDirect(byte[] buffer, int offset, int length)
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

  @Override
  public int read(long position, byte[] buffer, int offset, int length)
          throws IOException
  {
    synchronized (this) {
      long oldPos = getPos();
      try {
        seek(position);
        return readInternal(buffer, offset, length);
      }
      catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      catch (Exception e) {
        log.error(String.format("Failed to read from rubix for file %s position %d length %d. Falling back to remote", remotePath, nextReadPosition, length), e);
        getParentDataInputStream().readFully(position, buffer, offset, length);
        return length;
      }
      finally {
        seek(oldPos);
      }
    }
  }

  private int readInternal(byte[] buffer, int offset, int length)
      throws IOException, InterruptedException, ExecutionException

  {
    log.debug(String.format("Got Read, currentPos: %d currentBlock: %d bufferOffset: %d length: %d of file : %s", nextReadPosition, nextReadBlock, offset, length, remotePath));

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
        length,
        nextReadPosition,
        nextReadBlock);

    log.debug("Executing Chains");

    // start read requests
    ImmutableList.Builder builder = ImmutableList.builder();
    int sizeRead = 0;

    for (ReadRequestChain readRequestChain : readRequestChains) {
      readRequestChain.lock();
      builder.add(readService.submit(readRequestChain));
    }

    List<ListenableFuture<Long>> futures = builder.build();
    for (ListenableFuture<Long> future : futures) {
      // exceptions handled in caller
      try {
        sizeRead = Math.addExact(sizeRead, Math.toIntExact(future.get()));
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
    readService.execute(new Runnable() {
      @Override
      public void run()
      {
        updateCacheAndStats(readRequestChains);
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

  void updateCacheAndStats(final List<ReadRequestChain> readRequestChains)
  {
    ReadRequestChainStats stats = new ReadRequestChainStats();
    for (ReadRequestChain readRequestChain : readRequestChains) {
      readRequestChain.updateCacheStatus(remotePath, fileSize, lastModified, blockSize, conf);
      stats = stats.add(readRequestChain.getStats());
    }
    statsMbean.addReadRequestChainStats(stats);
  }

  List<ReadRequestChain> setupReadRequestChains(byte[] buffer,
                                   int offset,
                                   long endBlock,
                                   int length,
                                   long nextReadPosition,
                                   long nextReadBlock) throws IOException
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
    int generationNumber = UNKONWN_GENERATION_NUMBER;

    try (RetryingPooledBookkeeperClient bookKeeperClient = bookKeeperFactory.createBookKeeperClient(conf)) {
      CacheStatusRequest request = new CacheStatusRequest(remotePath, fileSize, lastModified,
          nextReadBlock, endBlock).setClusterType(clusterType.ordinal());
      request.setIncrMetrics(true);
      CacheStatusResponse response = bookKeeperClient.getCacheStatus(request);
      isCached = response.getBlocks();
      generationNumber = response.getGenerationNumber();
    }
    catch (Exception e) {
      if (strictMode) {
        throw Throwables.propagate(e);
      }
      log.debug("Could not get cache status from server ", e);
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

      lengthAlreadyConsidered += readRequest.getActualReadLengthIntUnsafe();

      if (isCached == null) {
        log.debug(String.format("Sending block %d to DirectReadRequestChain", blockNum));
        if (directReadRequestChain == null) {
          directReadRequestChain = new DirectReadRequestChain(getParentDataInputStream());
        }
        directReadRequestChain.addReadRequest(readRequest);
      }

      else if (isCached.get(idx).getLocation() == Location.CACHED) {
        log.debug(String.format("Sending cached block %d to cachedReadRequestChain", blockNum));
        if (cachedReadRequestChain == null) {
          cachedReadRequestChain = new CachedReadRequestChain(remoteFileSystem, remotePath, bufferPool, diskReadBufferSize,
                  statistics, conf, bookKeeperFactory, generationNumber);
        }
        cachedReadRequestChain.addReadRequest(readRequest);
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
                      statistics, nextReadBlock, endBlock, new BookKeeperFactory());
              nonLocalAsyncRequests.put(remoteLocation, nonLocalRequestChain);
            }
            nonLocalAsyncRequests.get(remoteLocation).addReadRequest(readRequest);
            if (nonLocalAsyncRequests.get(remoteLocation).needDirectReadRequest(blockNum)) {
              if (directReadRequestChain == null) {
                directReadRequestChain = new DirectReadRequestChain(getParentDataInputStream());
              }
              directReadRequestChain.addReadRequest(readRequest.clone(false));
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
          if (CacheConfig.isParallelWarmupEnabled(conf)) {
            log.debug(String.format("Sending block %d to remoteFetchRequestChain", blockNum));
            if (directReadRequestChain == null) {
              directReadRequestChain = new DirectReadRequestChain(getParentDataInputStream());
            }

            if (remoteFetchRequestChain == null) {
              remoteFetchRequestChain = new RemoteFetchRequestChain(remotePath, remoteFileSystem,
                  "localhost", conf, lastModified, fileSize, clusterType.ordinal(), bookKeeperFactory);
            }

            directReadRequestChain.addReadRequest(readRequest);
            remoteFetchRequestChain.addReadRequest(readRequest.clone(true));
          }
          else {
            log.debug(String.format("Sending block %d to remoteReadRequestChain", blockNum));
            if (affixBuffer == null) {
              affixBuffer = new byte[blockSize];
            }
            if (remoteReadRequestChain == null) {
              remoteReadRequestChain = new RemoteReadRequestChain(getParentDataInputStream(), remotePath, generationNumber, bufferPool, conf, affixBuffer, bookKeeperFactory);
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

  @Override
  public void close()
  {
    try {
      if (inputStream != null) {
        inputStream.close();
      }
    }
    catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }
}
