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

import com.qubole.rubix.spi.fop.Poolable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.TException;
import org.apache.thrift.TServiceClient;
import org.apache.thrift.transport.TTransport;

import java.io.Closeable;
import java.util.concurrent.Callable;

public abstract class RetryingPooledThriftClient
    implements Closeable
{
  private static final Log log = LogFactory.getLog(RetryingPooledThriftClient.class);

  private final int maxRetries;
  private final Configuration conf;
  private final String host;

  private Poolable<TTransport> transportPoolable;
  protected TServiceClient client;

  public RetryingPooledThriftClient(int maxRetries, Configuration conf, String host, Poolable<TTransport> transportPoolable)
  {
    this.maxRetries = maxRetries;
    this.conf = conf;
    this.host = host;
    this.transportPoolable = transportPoolable;
  }

  private void updateClient(Poolable<TTransport> transportPoolable)
  {
    this.client = setupClient(transportPoolable);
  }

  public abstract TServiceClient setupClient(Poolable<TTransport> transportPoolable);

  protected <V> V retryConnection(Callable<V> callable)
      throws TException
  {
    int errors = 0;

    if (client == null) {
      updateClient(transportPoolable);
    }

    while (errors < maxRetries) {
      try {
        return callable.call();
      }
      catch (Exception e) {
        log.warn("Error while connecting : ", e);
        errors++;
        // We dont want to keep the transport around in case of exception to prevent reading old results in transport reuse
        if (client.getInputProtocol().getTransport().isOpen()) {
          // Close connection and submit back so that ObjectPool to handle decommissioning
          client.getInputProtocol().getTransport().close();
          transportPoolable.getPool().returnObject(transportPoolable);
        }
        updateClient(transportPoolable.getPool().borrowObject(host, conf));
      }
    }

    throw new TException();
  }

  @Override
  public void close()
  {
    if (transportPoolable != null) {
      transportPoolable.getPool().returnObject(transportPoolable);
    }
  }
}
