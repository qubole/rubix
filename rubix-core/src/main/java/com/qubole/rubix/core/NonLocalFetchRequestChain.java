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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;

public class NonLocalFetchRequestChain extends ReadRequestChain
{
  private static final Log log = LogFactory.getLog(NonLocalFetchRequestChain.class);

  String remotePath;
  FileSystem remoteFileSystem;
  String remoteNodeLocation;
  BookKeeperFactory bookKeeperFactory;

  public NonLocalFetchRequestChain(String remotePath, FileSystem remoteFileSystem, String remoteNodeLocation)
  {
    this.remotePath = remotePath;
    this.remoteFileSystem = remoteFileSystem;
    this.remoteNodeLocation = remoteNodeLocation;
    this.bookKeeperFactory = bookKeeperFactory;
  }

  @Override
  public Integer call() throws Exception
  {
    return 0;
  }

  public ReadRequestChainStats getStats()
  {
    return new ReadRequestChainStats().setRemoteReads(requests);
  }
}