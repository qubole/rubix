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
import com.qubole.rubix.spi.BookKeeperFactory;
import com.qubole.rubix.spi.CacheConfig;
import com.qubole.rubix.spi.RetryingPooledBookkeeperClient;
import com.qubole.rubix.spi.thrift.BlockLocation;
import com.qubole.rubix.spi.thrift.CacheStatusRequest;
import com.qubole.rubix.spi.thrift.Location;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static com.qubole.rubix.spi.CacheUtil.UNKONWN_GENERATION_NUMBER;

public class NonLocalRequestChain extends ReadRequestChain
{
  private static final Log log = LogFactory.getLog(NonLocalRequestChain.class);

  String remoteNodeName;
  long fileSize;
  long lastModified;
  Configuration conf;
  FileSystem remoteFileSystem;
  String remoteFilePath;
  int clusterType;
  int blockSize;
  boolean strictMode;
  BookKeeperFactory bookKeeperFactory;
  NonLocalReadRequestChain nonLocalReadRequestChain;
  RemoteFetchRequestChain remoteFetchRequestChain;
  FileSystem.Statistics statistics;
  boolean needDirectReadRequest;
  List<BlockLocation> isCached;
  long startBlockForCacheStatus;
  long endBlockForCacheStatus;

  public NonLocalRequestChain(String remoteNodeName, long fileSize, long lastModified, Configuration conf,
                              FileSystem remoteFileSystem, String remoteFilePath, int clusterType,
                              boolean strictMode, FileSystem.Statistics statistics, long startBlock,
                              long endBlock, BookKeeperFactory bookKeeperFactory)
  {
    super(UNKONWN_GENERATION_NUMBER);
    this.remoteNodeName = remoteNodeName;
    this.remoteFileSystem = remoteFileSystem;
    this.lastModified = lastModified;
    this.remoteFilePath = remoteFilePath;
    this.fileSize = fileSize;
    this.conf = conf;
    this.clusterType = clusterType;
    this.strictMode = strictMode;
    this.statistics = statistics;
    this.startBlockForCacheStatus = startBlock;
    this.endBlockForCacheStatus = endBlock;

    this.bookKeeperFactory = bookKeeperFactory;
    this.blockSize = CacheConfig.getBlockSize(conf);

    try (RetryingPooledBookkeeperClient bookKeeperClient = bookKeeperFactory.createBookKeeperClient(remoteNodeName, conf)) {
      log.debug(" Trying to getCacheStatus from : " + remoteNodeName + " for file : " + remoteFilePath
              + " StartBlock : " + startBlock + " EndBlock : " + endBlock);

      CacheStatusRequest request = new CacheStatusRequest(remoteFilePath, fileSize, lastModified, startBlock,
          endBlock).setClusterType(clusterType);
      isCached = bookKeeperClient.getCacheStatus(request).getBlocks();
      log.debug("Cache Status : " + isCached);
    }
    catch (Exception e) {
      if (strictMode) {
        throw Throwables.propagate(e);
      }
      log.error("Could not get cache status from bookkeeper server at " + remoteNodeName, e);
    }
  }

  public ReadRequestChainStats getStats()
  {
    if (nonLocalReadRequestChain != null) {
      return new ReadRequestChainStats().setNonLocalReads(nonLocalReadRequestChain.requests);
    }
    else {
      // TODO: ReadRequestChainStats could collect number of direct reads from here
      return new ReadRequestChainStats();
    }
  }

  public void addReadRequest(ReadRequest readRequest)
  {
    long blockNum = readRequest.backendReadStart / blockSize;

    if (!needDirectReadRequest(blockNum)) {
      needDirectReadRequest = false;
      if (nonLocalReadRequestChain == null) {
        nonLocalReadRequestChain = new NonLocalReadRequestChain(remoteNodeName, fileSize, lastModified, conf,
            remoteFileSystem, remoteFilePath, clusterType, strictMode, statistics);
      }
      nonLocalReadRequestChain.addReadRequest(readRequest);
    }
    else {
      needDirectReadRequest = true;
      if (remoteFetchRequestChain == null) {
        remoteFetchRequestChain = new RemoteFetchRequestChain(remoteFilePath, remoteFileSystem, remoteNodeName,
            conf, lastModified, fileSize, clusterType, bookKeeperFactory);
      }
      remoteFetchRequestChain.addReadRequest(readRequest);
    }
  }

  protected boolean needDirectReadRequest(long blockNum)
  {
    int idx = (int) (blockNum - startBlockForCacheStatus);
    if (isCached != null && isCached.get(idx).getLocation() == Location.CACHED) {
      return false;
    }

    return true;
  }

  @Override
  public Long call() throws Exception
  {
    log.debug(String.format("Read Request threadName: %s, NonLocal Executor threadName: %s", threadName, Thread.currentThread().getName()));
    Thread.currentThread().setName(threadName);
    checkState(isLocked, "Trying to execute Chain without locking");
    long startTime = System.currentTimeMillis();

    long nonLocalReadBytes = 0;

    log.debug("Executing NonLocalRequestChain ");

    if (nonLocalReadRequestChain != null) {
      nonLocalReadRequestChain.lock();
      nonLocalReadBytes += nonLocalReadRequestChain.call();
    }
    // Send a request remote node bookkeeper to fetch the data in async mode
    if (remoteFetchRequestChain != null) {
      remoteFetchRequestChain.call();
    }

    log.debug("NonLocalRequest took : " + (System.currentTimeMillis() - startTime) + " msecs ");

    return nonLocalReadBytes;
  }

  @Override
  public void updateCacheStatus(String remotePath, long fileSize, long lastModified, int blockSize, Configuration conf)
  {
    if (CacheConfig.isDummyModeEnabled(conf)) {
      if (remoteFetchRequestChain == null || remoteFetchRequestChain.getReadRequests().isEmpty()) {
        return;
      }
      remoteFetchRequestChain.updateCacheStatus(remotePath, fileSize, lastModified, blockSize, conf);
    }
  }
}
