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

import com.qubole.rubix.spi.BookKeeperFactory;
import com.qubole.rubix.spi.thrift.BookKeeperService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.nio.ByteBuffer;

public final class ReadRequestChainFactory
{
  private ReadRequestChainFactory()
  { }

  public static <T extends ReadRequestChain> T createReadRequestChain(Class<T> readRequestChainType, FSDataInputStream inputStream)
  {
    if (readRequestChainType == DirectReadRequestChain.class) {
      return readRequestChainType.cast(new DirectReadRequestChain(inputStream));
    }
    throw new IllegalArgumentException("The type of ReadRequestChain: " + readRequestChainType.getName() + " does not match the arguments or is unknown");
  }

  public static <T extends ReadRequestChain> T createReadRequestChain(Class<T> readRequestChainType, FileSystem remoteFileSystem, String remotePath, ByteBuffer buffer,
                                                               FileSystem.Statistics statistics, Configuration conf, BookKeeperFactory bookKeeperFactory) throws IOException
  {
    if (readRequestChainType == CachedReadRequestChain.class) {
      return readRequestChainType.cast(new CachedReadRequestChain(remoteFileSystem, remotePath, buffer, statistics, conf, bookKeeperFactory));
    }
    throw new IllegalArgumentException("The type of ReadRequestChain: " + readRequestChainType.getName() + " does not match the arguments or is unknown");
  }

  public static <T extends ReadRequestChain> T createReadRequestChain(Class<T> readRequestChainType, String remoteLocation, long fileSize, long lastModified, Configuration conf,
                                                               FileSystem remoteFileSystem, String remotePath, int clusterType,
                                                               boolean strictMode, FileSystem.Statistics statistics, long startBlock, long endBlock)
  {
    if (readRequestChainType == NonLocalReadRequestChain.class) {
      return readRequestChainType.cast(new NonLocalReadRequestChain(remoteLocation, fileSize, lastModified, conf, remoteFileSystem, remotePath, clusterType, strictMode, statistics));
    }
    else if (readRequestChainType == NonLocalRequestChain.class) {
      return readRequestChainType.cast(new NonLocalRequestChain(remoteLocation, fileSize, lastModified, conf, remoteFileSystem, remotePath, clusterType, strictMode, statistics, startBlock, endBlock));
    }
    else if (readRequestChainType == RemoteFetchRequestChain.class) {
      return readRequestChainType.cast(new RemoteFetchRequestChain(remotePath, remoteFileSystem, remoteLocation, conf, lastModified, fileSize, clusterType));
    }
    throw new IllegalArgumentException("The type of ReadRequestChain: " + readRequestChainType.getName() + " does not match the arguments or is unknown");
  }

  public static <T extends ReadRequestChain> T createReadRequestChain(Class<T> readRequestChainType, FSDataInputStream inputStream, String localfile, ByteBuffer directBuffer, byte[] affixBuffer, BookKeeperFactory bookKeeperFactory) throws IOException
  {
    if (readRequestChainType == RemoteReadRequestChain.class) {
      if (bookKeeperFactory == null) {
        bookKeeperFactory = new BookKeeperFactory();
      }
      return readRequestChainType.cast(new RemoteReadRequestChain(inputStream, localfile, directBuffer, affixBuffer, bookKeeperFactory));
    }
    throw new IllegalArgumentException("The type of ReadRequestChain: " + readRequestChainType.getName() + " does not match the arguments or is unknown");
  }

  public static <T extends ReadRequestChain> T createReadRequestChain(Class<T> readRequestChainType, BookKeeperService.Client bookKeeperClient, FileSystem remoteFileSystem, String localfile,
                                                               ByteBuffer directBuffer, Configuration conf, String remotePath,
                                                               long fileSize, long lastModified)
  {
    if (readRequestChainType == FileDownloadRequestChain.class) {
      return readRequestChainType.cast(new FileDownloadRequestChain(bookKeeperClient, remoteFileSystem, localfile, directBuffer, conf, remotePath, fileSize, lastModified));
    }
    throw new IllegalArgumentException("The type of ReadRequestChain: " + readRequestChainType.getName() + " does not match the arguments or is unknown");
  }
}
