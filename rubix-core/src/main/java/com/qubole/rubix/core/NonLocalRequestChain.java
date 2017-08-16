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
import com.qubole.rubix.spi.RetryingBookkeeperClient;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

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
  boolean strictMode;
  BookKeeperFactory bookKeeperFactory;

  DirectReadRequestChain directReadChain = null;
  int directRead = 0;

  public NonLocalRequestChain(String remoteNodeName, long fileSize, long lastModified, Configuration conf,
                              FileSystem remoteFileSystem, String remoteFilePath, FSDataInputStream inputStream,
                              int clusterType, boolean strictMode)
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
  }

  public ReadRequestChainStats getStats()
  {
    return new ReadRequestChainStats().setRemoteReads(requests);
  }

  @Override
  public Integer call() throws Exception
  {
    Thread.currentThread().setName(threadName);
    if (readRequests.size() == 0) {
      return 0;
    }
    checkState(isLocked, "Trying to execute Chain without locking");

    NonLocalReadRequestChain nonLocalReadRequestChain = null;
    DirectReadRequestChain directReadRequestChain = null;

    for (ReadRequest readRequest : readRequests) {
      if (cancelled) {
        propagateCancel(this.getClass().getName());
      }
      RetryingBookkeeperClient bookKeeperCleint;
      try {
        bookKeeperCleint = bookKeeperFactory.createBookKeeperClient(remoteNodeName, conf);
        List<BlockLocation> isCached = null;
        isCached = bookKeeperCleint.getCacheStatus(remoteFilePath, fileSize, lastModified,
            readRequest.backendReadStart,readRequest.backendReadEnd, clusterType);

        int idx = 0;
        for (long blockNum = readRequest.backendReadStart; blockNum < readRequest.backendReadEnd; blockNum ++, idx++) {
          if (isCached.get(idx).getLocation() == Location.CACHED) {
            if (nonLocalReadRequestChain == null) {
              nonLocalReadRequestChain = new NonLocalReadRequestChain(remoteNodeName, fileSize, lastModified, conf,
                  remoteFileSystem, remoteFilePath, clusterType, strictMode);
            }
            nonLocalReadRequestChain.addReadRequest(readRequest);

          } else {
            if (directReadRequestChain == null) {
              directReadRequestChain = new DirectReadRequestChain(inputStream);
            }
            directReadRequestChain.addReadRequest(readRequest);
          }
        }
      } catch (Exception e) {
        log.warn("Could not create BookKeeper Client ", e);
        if (strictMode) {
          throw Throwables.propagate(e);
        }
        else {
          return directReadRequest(readRequests.indexOf(readRequest));
        }
      }
    }
    return 0;
  }

  private int directReadRequest(int index)
      throws Exception
  {
    FSDataInputStream inputStream = remoteFileSystem.open(new Path(remoteFilePath));
    directReadChain = new DirectReadRequestChain(inputStream);
    for (ReadRequest readRequest : readRequests.subList(index, readRequests.size())) {
      directReadChain.addReadRequest(readRequest);
    }
    directReadChain.lock();
    directRead = directReadChain.call();
    inputStream.close();
    directReadChain = null;
    return directRead;
  }
}