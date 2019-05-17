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

  public static DirectReadRequestChain createDirectReadRequestChain(FSDataInputStream inputStream)
  {
    return new DirectReadRequestChain(inputStream);
  }

  public static CachedReadRequestChain createCachedReadRequestChain(FileSystem remoteFileSystem,
                                                                    String remotePath,
                                                                    ByteBuffer buffer,
                                                                    FileSystem.Statistics statistics,
                                                                    Configuration conf,
                                                                    BookKeeperFactory bookKeeperFactory)
          throws IOException
  {
    return new CachedReadRequestChain(remoteFileSystem, remotePath, buffer, statistics, conf, bookKeeperFactory);
  }

  public static NonLocalReadRequestChain createNonLocalReadRequestChain(String remoteLocation,
                                                                        long fileSize,
                                                                        long lastModified,
                                                                        Configuration conf,
                                                                        FileSystem remoteFileSystem,
                                                                        String remotePath,
                                                                        int clusterType,
                                                                        boolean strictMode,
                                                                        FileSystem.Statistics statistics)
  {
    return new NonLocalReadRequestChain(remoteLocation, fileSize, lastModified, conf, remoteFileSystem, remotePath, clusterType, strictMode, statistics);
  }

  public static RemoteReadRequestChain createRemoteReadRequestChain(FSDataInputStream inputStream,
                                                                    String localFile,
                                                                    ByteBuffer directBuffer,
                                                                    byte[] affixBuffer)
          throws IOException
  {
    return createRemoteReadRequestChain(inputStream, localFile, directBuffer, affixBuffer, new BookKeeperFactory());
  }

  public static RemoteReadRequestChain createRemoteReadRequestChain(FSDataInputStream inputStream,
                                                                    String localFile,
                                                                    ByteBuffer directBuffer,
                                                                    byte[] affixBuffer,
                                                                    BookKeeperFactory bookKeeperFactory)
          throws IOException
  {
    return new RemoteReadRequestChain(inputStream, localFile, directBuffer, affixBuffer, bookKeeperFactory);
  }

  public static NonLocalRequestChain createNonLocalRequestChain(String remoteLocation,
                                                                long fileSize,
                                                                long lastModified,
                                                                Configuration conf,
                                                                FileSystem remoteFileSystem,
                                                                String remotePath,
                                                                int clusterType,
                                                                boolean strictMode,
                                                                FileSystem.Statistics statistics,
                                                                long startBlock,
                                                                long endBlock)
  {
    return new NonLocalRequestChain(remoteLocation, fileSize, lastModified, conf, remoteFileSystem, remotePath, clusterType, strictMode, statistics, startBlock, endBlock);
  }

  public static RemoteFetchRequestChain createRemoteFetchRequestChain(String remoteLocation,
                                                                      long fileSize,
                                                                      long lastModified,
                                                                      Configuration conf,
                                                                      FileSystem remoteFileSystem,
                                                                      String remotePath,
                                                                      int clusterType)
  {
    return new RemoteFetchRequestChain(remotePath, remoteFileSystem, remoteLocation, conf, lastModified, fileSize, clusterType);
  }

  public static FileDownloadRequestChain createFileDownloadRequestChain(BookKeeperService.Client bookKeeperClient,
                                                                        FileSystem remoteFileSystem,
                                                                        String localFile,
                                                                        ByteBuffer directBuffer,
                                                                        Configuration conf,
                                                                        String remotePath,
                                                                        long fileSize,
                                                                        long lastModified)
  {
    return new FileDownloadRequestChain(bookKeeperClient, remoteFileSystem, localFile, directBuffer, conf, remotePath, fileSize, lastModified);
  }
}
