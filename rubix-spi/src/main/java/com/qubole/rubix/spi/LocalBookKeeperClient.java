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
import com.qubole.rubix.spi.thrift.ClusterNode;
import com.qubole.rubix.spi.thrift.FileInfo;
import com.qubole.rubix.spi.thrift.ReadDataRequest;
import com.qubole.rubix.spi.thrift.SetCachedRequest;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransport;

import java.util.List;
import java.util.Map;

/**
 * Created by sakshia on 6/10/16.
 */
public class LocalBookKeeperClient extends RetryingBookkeeperClient
{
  private static final Log log = LogFactory.getLog(RetryingBookkeeperClient.class);
  BookKeeperService.Iface bookKeeper;

  public LocalBookKeeperClient(TTransport transport, BookKeeperService.Iface bookKeeper)
  {
    super(transport, 1);
    this.bookKeeper = bookKeeper;
  }

  @Override
  public List<BlockLocation> getCacheStatus(CacheStatusRequest request) throws TException
  {
    return bookKeeper.getCacheStatus(request);
  }

  @Override
  public void setAllCached(SetCachedRequest request)
      throws TException
  {
    bookKeeper.setAllCached(request);
  }

  @Override
  public Map<String, Double> getCacheMetrics()
          throws TException
  {
    return bookKeeper.getCacheMetrics();
  }

  @Override
  public boolean readData(ReadDataRequest request)
          throws TException
  {
    return bookKeeper.readData(request);
  }

  @Override
  public FileInfo getFileInfo(String remotePath)
          throws TException
  {
    return bookKeeper.getFileInfo(remotePath);
  }

  @Override
  public List<ClusterNode> getClusterNodes()
          throws TException
  {
    return bookKeeper.getClusterNodes();
  }

  @Override
  public String getOwnerNodeForPath(String remotePathKey)
          throws TException
  {
    return bookKeeper.getOwnerNodeForPath(remotePathKey);
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
}
