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
package com.qubole.rubix.spi;

import com.qubole.rubix.spi.thrift.BlockLocation;
import com.qubole.rubix.spi.thrift.BookKeeperService;
import com.qubole.rubix.spi.thrift.CacheStatusRequest;
import com.qubole.rubix.spi.thrift.FileInfo;
import com.qubole.rubix.spi.thrift.HeartbeatStatus;
import org.apache.thrift.TException;

import java.util.List;
import java.util.Map;

/**
 * Created by sakshia on 6/10/16.
 */
public class LocalBookKeeperClient extends RetryingPooledBookkeeperClient
{
  BookKeeperService.Iface bookKeeper;

  public LocalBookKeeperClient(BookKeeperService.Iface bookKeeper)
  {
    super();
    this.bookKeeper = bookKeeper;
  }

  @Override
  public List<BlockLocation> getCacheStatus(CacheStatusRequest request) throws TException
  {
    return bookKeeper.getCacheStatus(request);
  }

  @Override
  public void setAllCached(final String remotePath, final long fileLength, final long lastModified, final long startBlock, final long endBlock)
      throws TException
  {
    bookKeeper.setAllCached(remotePath, fileLength, lastModified, startBlock, endBlock);
  }

  @Override
  public Map<String, Double> getCacheMetrics()
          throws TException
  {
    return bookKeeper.getCacheMetrics();
  }

  @Override
  public boolean readData(String path, long readStart, int length, long fileSize, long lastModified, int clusterType)
          throws TException
  {
    return bookKeeper.readData(path, readStart, length, fileSize, lastModified, clusterType);
  }

  @Override
  public void handleHeartbeat(String workerHostname, HeartbeatStatus heartbeatStatus)
          throws TException
  {
    bookKeeper.handleHeartbeat(workerHostname, heartbeatStatus);
  }

  @Override
  public FileInfo getFileInfo(String remotePath)
          throws TException
  {
    return bookKeeper.getFileInfo(remotePath);
  }

  @Override
  public boolean isBookKeeperAlive()
          throws TException
  {
    return bookKeeper.isBookKeeperAlive();
  }

  @Override
  public void invalidateFileMetadata(String remotePath)
          throws TException
  {
    bookKeeper.invalidateFileMetadata(remotePath);
  }

  @Override
  public void close()
  {
    // no-op
  }
}
