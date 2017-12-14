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

import com.qubole.rubix.spi.BookKeeperFactory;
import com.qubole.rubix.spi.RetryingBookkeeperClient;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

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
  int clusterType;

  public RemoteFetchRequestChain(String remotePath, FileSystem remoteFileSystem, String remoteNodeLocation,
                                 Configuration conf, long lastModified, long fileSize, int clusterType)
  {
    this.remotePath = remotePath;
    this.remoteFileSystem = remoteFileSystem;
    this.remoteNodeLocation = remoteNodeLocation;
    this.conf = conf;
    this.lastModified = lastModified;
    this.fileSize = fileSize;
    this.clusterType = clusterType;
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
        client.readData(remotePath, request.backendReadStart, request.getBackendReadLength(),
            fileSize, lastModified, clusterType);
      }
    }
    finally {
      client.close();
      client = null;
    }
    log.info("Send request to remote took " + (System.currentTimeMillis() - startTime) + " :msecs");

    return 0;
  }

  public ReadRequestChainStats getStats()
  {
    return new ReadRequestChainStats().setRemoteReads(requests);
  }
}
