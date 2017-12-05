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
import com.qubole.rubix.spi.BlockLocation;
import com.qubole.rubix.spi.BookKeeperFactory;
import com.qubole.rubix.spi.Location;
import com.qubole.rubix.spi.CacheConfig;
import com.qubole.rubix.spi.RetryingBookkeeperClient;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;

import java.util.List;

import static com.google.common.base.Preconditions.checkState;

public class NonLocalRequestChain extends ReadRequestChain
{
  private static final Log log = LogFactory.getLog(NonLocalRequestChain.class);

  String remoteNodeName;
  long fileSize;
  long lastModified;
  Configuration conf;
  FileSystem remoteFileSystem;
  String remoteFilePath;
  FSDataInputStream inputStream;
  int clusterType;
  int blockSize;
  boolean strictMode;
  BookKeeperFactory bookKeeperFactory;
  RetryingBookkeeperClient bookKeeperClient;
  NonLocalReadRequestChain nonLocalReadRequestChain = null;
  DirectReadRequestChain directReadRequestChain = null;
  NonLocalFetchRequestChain nonLocalFetchRequestChain = null;
  FileSystem.Statistics statistics = null;

  int directRead = 0;

  public NonLocalRequestChain(String remoteNodeName, long fileSize, long lastModified, Configuration conf,
                              FileSystem remoteFileSystem, String remoteFilePath, FSDataInputStream inputStream,
                              int clusterType, boolean strictMode, FileSystem.Statistics statistics)
  {
    this.remoteNodeName = remoteNodeName;
    this.remoteFileSystem = remoteFileSystem;
    this.lastModified = lastModified;
    this.remoteFilePath = remoteFilePath;
    this.inputStream = inputStream;
    this.fileSize = fileSize;
    this.conf = conf;
    this.clusterType = clusterType;
    this.strictMode = strictMode;
    this.bookKeeperFactory = new BookKeeperFactory();
    this.blockSize = CacheConfig.getBlockSize(conf);
  }

  public ReadRequestChainStats getStats()
  {
    return new ReadRequestChainStats().setRemoteReads(requests);
  }

  public void addReadRequest(ReadRequest readRequest)
  {
    List<BlockLocation> isCached = null;
    long startBlock = readRequest.backendReadStart / blockSize;
    long endBlock = (readRequest.backendReadEnd / blockSize) == startBlock ? startBlock + 1 :
        (readRequest.backendReadEnd / blockSize);

    // TODO Optimize not to make remote cache status call everytime
    try {
      this.bookKeeperClient = bookKeeperFactory.createBookKeeperClient(remoteNodeName, conf);
      log.info(" Trying to getCacheStatus from : " + remoteNodeName + " for file : " + remoteFilePath
          + " StartBlock : " + startBlock + " EndBlock : " + endBlock + " backendReadStart : "
          + readRequest.backendReadStart + " backendReadEnd : " + readRequest.backendReadEnd);

      isCached = bookKeeperClient.getCacheStatus(remoteFilePath, fileSize, lastModified,
            startBlock, endBlock, clusterType);
    }
    catch (Exception e) {
      if (strictMode) {
        throw Throwables.propagate(e);
      }
      log.info("Could not get cache status from server " + Throwables.getStackTraceAsString(e));
    }
    finally {
      try {
        bookKeeperClient.close();
        bookKeeperClient = null;
      }
      catch (Exception e) {
        log.info("Could not close BookKeeper client " + Throwables.getStackTraceAsString(e));
      }
    }

    log.info("Cache Status : " + isCached);

    int idx = 0;
    for (long blockNum = startBlock; blockNum < endBlock; blockNum++, idx++) {
      if (isCached != null && isCached.get(idx).getLocation() == Location.CACHED) {
        if (nonLocalReadRequestChain == null) {
          nonLocalReadRequestChain = new NonLocalReadRequestChain(remoteNodeName, fileSize, lastModified, conf,
              remoteFileSystem, remoteFilePath, clusterType, strictMode, statistics);
        }
        nonLocalReadRequestChain.addReadRequest(readRequest);
      }
      else {
        if (directReadRequestChain == null) {
          log.info("Initilaizing Direct Read");
          directReadRequestChain = new DirectReadRequestChain(inputStream);
        }
        directReadRequestChain.addReadRequest(readRequest);

        if (nonLocalFetchRequestChain == null) {
          nonLocalFetchRequestChain = new NonLocalFetchRequestChain(remoteFilePath, remoteFileSystem, remoteNodeName,
              conf, lastModified, fileSize, clusterType);
        }
        nonLocalFetchRequestChain.addReadRequest(readRequest);
      }
    }
  }

  @Override
  public Integer call() throws Exception
  {
    Thread.currentThread().setName(threadName);
    checkState(isLocked, "Trying to execute Chain without locking");

    int nonLocalReadBytes = 0;
    int directReadBytes = 0;

    log.info("Executing NonLocalRequestChain ");
    if (directReadRequestChain != null) {
      directReadRequestChain.lock();
      directReadBytes += directReadRequestChain.call();
    }

    if (nonLocalReadRequestChain != null) {
      nonLocalReadRequestChain.lock();
      nonLocalReadBytes += nonLocalReadRequestChain.call();
    }
    // Send a request remote node bookkeeper to fetch the data in async mode
    if (nonLocalFetchRequestChain != null) {
      nonLocalFetchRequestChain.call();
    }

    return nonLocalReadBytes + directReadBytes;
  }
}
