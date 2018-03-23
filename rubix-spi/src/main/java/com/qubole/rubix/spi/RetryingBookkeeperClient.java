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
package com.qubole.rubix.spi;

/**
 * Created by sakshia on 27/9/16.
 */

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;

public class RetryingBookkeeperClient extends BookKeeperService.Client implements Closeable
{
  private static final Logger LOG = LoggerFactory.getLogger(RetryingBookkeeperClient.class);
  private int maxRetries;
  TTransport transport;

  public RetryingBookkeeperClient(TTransport transport, int maxRetries)
  {
    super(new TBinaryProtocol(transport));
    this.transport = transport;
    this.maxRetries = maxRetries;
  }

  @Override
  public List<BlockLocation> getCacheStatus(final String remotePath, final long fileLength, final long lastModified, final long startBlock, final long endBlock, final int clusterType)
      throws TException
  {
    return retryConnection(new Callable<List<BlockLocation>>()
    {
      @Override
      public List<BlockLocation> call()
          throws TException
      {
        return RetryingBookkeeperClient.super.getCacheStatus(remotePath, fileLength, lastModified, startBlock, endBlock, clusterType);
      }
    });
  }

  @Override
  public void setAllCached(final String remotePath, final long fileLength, final long lastModified, final long startBlock, final long endBlock)
      throws TException
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

  private <V> V retryConnection(Callable<V> callable)
      throws TException
  {
    int errors = 0;

    while (errors < maxRetries) {
      try {
        if (!transport.isOpen()) {
          transport.open();
        }
        return callable.call();
      }
      catch (Exception e) {
        LOG.info("Error while connecting : ", e);
        errors++;
      }
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
    transport.close();
  }
}
