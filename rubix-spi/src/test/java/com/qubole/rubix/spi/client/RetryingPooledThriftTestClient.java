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
package com.qubole.rubix.spi.client;

import com.qubole.rubix.spi.RetryingPooledThriftClient;
import com.qubole.rubix.spi.fop.Poolable;
import com.qubole.rubix.spi.thrift.Request;
import com.qubole.rubix.spi.thrift.TestingService;
import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.TException;
import org.apache.thrift.TServiceClient;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TTransport;

import java.util.concurrent.Callable;

public class RetryingPooledThriftTestClient
    extends RetryingPooledThriftClient
    implements TestingService.Iface
{
  public RetryingPooledThriftTestClient(int maxRetries, Configuration conf, String host, Poolable<TTransport> transportPoolable)
  {
    super(maxRetries, conf, host, transportPoolable);
  }

  @Override
  public TServiceClient setupClient(Poolable<TTransport> transportPoolable)
  {
    return new TestingService.Client(new TBinaryProtocol(transportPoolable.getObject()));
  }

  @Override
  public String echo(final Request request)
      throws TException
  {
    return retryConnection(new Callable<String>()
    {
      @Override
      public String call()
          throws TException
      {
        return ((TestingService.Client) client).echo(request);
      }
    });
  }
}
