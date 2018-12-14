/**
 * Copyright (c) 2018. Qubole Inc
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

/**
 * Created by sakshia on 27/9/16.
 */

import com.qubole.rubix.spi.thrift.BlockLocation;
import com.qubole.rubix.spi.thrift.BookKeeperService;
import com.qubole.rubix.spi.thrift.CacheStatusRequest;
import com.qubole.rubix.spi.thrift.HeartbeatStatus;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TTransport;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;

public class RetryingBookkeeperClient extends BookKeeperService.Client implements Closeable
{
  private static final Log log = LogFactory.getLog(RetryingBookkeeperClient.class);
  private int maxRetries;
  private long retryInterval;
  TTransport transport;

  public RetryingBookkeeperClient(TTransport transport, int maxRetries, long retryInterval)
  {
    super(new TBinaryProtocol(transport));
    this.transport = transport;
    this.maxRetries = maxRetries;
    this.retryInterval = retryInterval;
  }

  @Override
  public List<BlockLocation> getCacheStatus(final CacheStatusRequest request) throws TException
  {
    return retryConnection(new Callable<List<BlockLocation>>()
    {
      @Override
      public List<BlockLocation> call()
          throws TException
      {
        return RetryingBookkeeperClient.super.getCacheStatus(request);
      }
    });
  }

  @Override
  public boolean readData(final String remotePath, final long offset, final int length, final long fileSize,
                          final long lastModified, final int clusterType) throws TException
  {
    return retryConnection(new Callable<Boolean>()
    {
      @Override
      public Boolean call() throws Exception
      {
        return RetryingBookkeeperClient.super.readData(remotePath, offset, length, fileSize, lastModified, clusterType);
      }
    });
  }

  @Override
  public void setAllCached(final String remotePath, final long fileLength, final long lastModified,
                           final long startBlock, final long endBlock) throws TException
  {
    retryConnection(new Callable<Void>()
    {
      @Override
      public Void call()
          throws Exception
      {
        RetryingBookkeeperClient.super.setAllCached(remotePath, fileLength, lastModified, startBlock, endBlock);
        return null;
      }
    });
  }

  @Override
  public void handleHeartbeat(final String workerHostname, final HeartbeatStatus heartbeatStatus) throws TException
  {
    retryConnection(new Callable<Void>()
    {
      @Override
      public Void call() throws Exception
      {
        RetryingBookkeeperClient.super.handleHeartbeat(workerHostname, heartbeatStatus);
        return null;
      }
    });
  }

  @Override
  public String getClusterNodeHostName(final String remotePath, final int clusterType) throws TException
  {
    return retryConnection(new Callable<String>()
    {
      @Override
      public String call() throws Exception
      {
        return RetryingBookkeeperClient.super.getClusterNodeHostName(remotePath, clusterType);
      }
    });
  }

  @Override
  public List<String> getNodeHostNames(final int clusterType) throws TException
  {
    return retryConnection(new Callable<List<String>>()
    {
      @Override
      public List<String> call() throws Exception
      {
        return RetryingBookkeeperClient.super.getNodeHostNames(clusterType);
      }
    });
  }

  private <V> V retryConnection(Callable<V> callable)
      throws TException
  {
    int errors = 0;

    try {
      while (errors < maxRetries) {
        try {
          if (!transport.isOpen()) {
            transport.open();
          }
          return callable.call();
        }
        catch (Exception e) {
          log.debug(String.format("Error while contacting BookKeeper. Tried [%d/%d attempts] ", errors + 1, maxRetries));
          errors++;
        }
        if (transport.isOpen()) {
          transport.close();
        }

        try {
          Thread.sleep(retryInterval);
        }
        catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
      log.error(String.format("Exhausted all of %d attempts to contact BookKeeper", maxRetries));
    }

    finally {
      if (transport.isOpen()) {
        transport.close();
      }
    }

    throw new TException();
  }

  @Override
  public void close()
      throws IOException
  {
    if (!transport.isOpen()) {
      transport.close();
    }
  }
}
