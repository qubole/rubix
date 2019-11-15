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

import com.qubole.rubix.spi.thrift.BookKeeperService;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

/**
 * Created by sakshia on 5/10/16.
 */
public class BookKeeperFactory
{
  BookKeeperService.Iface bookKeeper;
  private static Log log = LogFactory.getLog(BookKeeperFactory.class.getName());

  public BookKeeperFactory()
  {
  }

  public BookKeeperFactory(BookKeeperService.Iface bookKeeper)
  {
    if (bookKeeper != null) {
      this.bookKeeper = bookKeeper;
    }
  }

  public RetryingBookkeeperClient createBookKeeperClient(String host, Configuration conf) throws TTransportException
  {
    if (bookKeeper != null) {
      return new LocalBookKeeperClient(null, bookKeeper);
    }
    else {
      final int socketTimeout = CacheConfig.getServerSocketTimeout(conf);
      final int connectTimeout = CacheConfig.getServerConnectTimeout(conf);

      TTransport transport = new TSocket(host, CacheConfig.getServerPort(conf), socketTimeout, connectTimeout);
      transport.open();
      RetryingBookkeeperClient retryingBookkeeperClient = new RetryingBookkeeperClient(transport, CacheConfig.getMaxRetries(conf));
      return retryingBookkeeperClient;
    }
  }

  public RetryingBookkeeperClient createBookKeeperClient(String host, Configuration conf, int maxRetries,
                                                         long retryInterval, boolean throwException)
  {
    for (int failedStarts = 1; failedStarts <= maxRetries; failedStarts++) {
      try {
        return this.createBookKeeperClient(host, conf);
      }
      catch (TTransportException e) {
        log.warn(String.format("Could not create bookkeeper client [%d/%d attempts]", failedStarts, maxRetries));
      }
      try {
        Thread.sleep(retryInterval);
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    log.fatal("Ran out of retries to create bookkeeper client.");
    if (throwException) {
      throw new RuntimeException("Could not create bookkeeper client");
    }

    return null;
  }

  public boolean isBookKeeperInitialized()
  {
    return bookKeeper != null;
  }

  public RetryingBookkeeperClient createBookKeeperClient(Configuration conf) throws TTransportException
  {
    return createBookKeeperClient("localhost", conf);
  }
}
