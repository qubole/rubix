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
import com.qubole.rubix.spi.RetryingBookkeeperClient;
import com.qubole.rubix.spi.thrift.CacheStatusRequest;
import com.qubole.rubix.spi.thrift.ReadDataRequest;
import com.qubole.rubix.spi.thrift.SetCachedRequest;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;

public class RemoteFetchRequestChain extends ReadRequestChain
{
  private static final Log log = LogFactory.getLog(RemoteFetchRequestChain.class);

  String remotePath;
  FileSystem remoteFileSystem;
  String remoteNodeLocation;
  Configuration conf;
  BookKeeperFactory bookKeeperFactory;
  long lastModified;
  long fileSize;

  public RemoteFetchRequestChain(String remotePath, FileSystem remoteFileSystem, String remoteNodeLocation,
                                 Configuration conf, long lastModified, long fileSize)
  {
    this.remotePath = remotePath;
    this.remoteFileSystem = remoteFileSystem;
    this.remoteNodeLocation = remoteNodeLocation;
    this.conf = conf;
    this.lastModified = lastModified;
    this.fileSize = fileSize;
    this.bookKeeperFactory = new BookKeeperFactory();
  }

  @Override
  public Integer call() throws Exception
  {
    if (readRequests.size() == 0) {
      return 0;
    }
    long startTime = System.currentTimeMillis();

    RetryingBookkeeperClient client = null;
    try {
      client = bookKeeperFactory.createBookKeeperClient(remoteNodeLocation, conf);
      for (ReadRequest request : readRequests) {
        log.info("RemoteFetchRequest from : " + remoteNodeLocation + " Start : " + request.backendReadStart +
                " of length " + request.getBackendReadLength());
        client.readData(new ReadDataRequest(remotePath, request.backendReadStart, request.getBackendReadLength(),
            fileSize, lastModified));
      }
    }
    finally {
      try {
        if (client != null) {
          client.close();
          client = null;
        }
      }
      catch (IOException ex) {
        log.error("Could not close bookkeeper client. Exception: " + ex.toString());
      }
    }
    log.info("Send request to remote took " + (System.currentTimeMillis() - startTime) + " :msecs");

    return 0;
  }

  public ReadRequestChainStats getStats()
  {
    return new ReadRequestChainStats().setRemoteReads(requests);
  }

  @Override
  public void updateCacheStatus(String remotePath, long fileSize, long lastModified, int blockSize, Configuration conf)
  {
    if (CacheConfig.isDummyModeEnabled(conf)) {
      RetryingBookkeeperClient bookKeeperClient = null;
      try {
        bookKeeperClient = new BookKeeperFactory().createBookKeeperClient(remoteNodeLocation, conf);
        for (ReadRequest readRequest : readRequests) {
          long startBlock = toBlock(readRequest.getBackendReadStart());
          long endBlock = toBlock(readRequest.getBackendReadEnd() - 1) + 1;
          // getCacheStatus() call required to create mdfiles before blocks are set as cached
          CacheStatusRequest request = new CacheStatusRequest(remotePath, fileSize, lastModified, startBlock, endBlock);
          bookKeeperClient.getCacheStatus(request);
          bookKeeperClient.setAllCached(new SetCachedRequest(remotePath, fileSize, lastModified, startBlock, endBlock));
        }
      }
      catch (Exception e) {
        log.error("Dummy Mode: Could not update Cache Status for Remote Fetch Request " + Throwables.getStackTraceAsString(e));
      }
      finally {
        try {
          if (bookKeeperClient != null) {
            bookKeeperClient.close();
          }
        }
        catch (IOException ex) {
          log.error("Dummy Mode: Could not close bookkeeper client. Exception: " + ex.toString());
        }
      }
    }
  }

  private long toBlock(long pos)
  {
    long blockSize = CacheConfig.getBlockSize(conf);
    return pos / blockSize;
  }
}
