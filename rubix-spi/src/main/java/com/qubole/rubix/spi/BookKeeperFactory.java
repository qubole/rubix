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

import com.qubole.rubix.spi.fop.ObjectFactory;
import com.qubole.rubix.spi.fop.ObjectPool;
import com.qubole.rubix.spi.fop.PoolConfig;
import com.qubole.rubix.spi.fop.Poolable;
import com.qubole.rubix.spi.thrift.BookKeeperService;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by sakshia on 5/10/16.
 */
public class BookKeeperFactory
{
  BookKeeperService.Iface bookKeeper;
  private static Log log = LogFactory.getLog(BookKeeperFactory.class.getName());
  private static final AtomicBoolean initFlag = new AtomicBoolean();
  private static PoolConfig poolConfig;
  private static ObjectFactory<TSocket> factory;
  static ObjectPool pool;

  private static final String LOCALHOST = "localhost";

  private void init(final Configuration conf)
  {
    poolConfig = new PoolConfig();
    poolConfig.setMaxSize(CacheConfig.getPoolSizeMax(conf));
    poolConfig.setMinSize(CacheConfig.getPoolSizeMin(conf));
    poolConfig.setDelta(CacheConfig.getPoolDeltaSize(conf));
    poolConfig.setMaxWaitMilliseconds(CacheConfig.getPoolMaxWait(conf));

    factory = new ObjectFactory<TSocket>()
    {
      @Override
      public TSocket create(String host, int socketTimeout, int connectTimeout)
      {
        log.debug("Opening connection to host: " + host);
        TSocket socket = null;
        try {
          socket = new TSocket(host, CacheConfig.getBookKeeperServerPort(conf), socketTimeout, connectTimeout);
          socket.open();
        }
        catch (TTransportException e) {
          e.printStackTrace();
        }
        return socket;
      }

      @Override
      public void destroy(TSocket o)
      {
        // clean up and release resources
        o.close();
      }

      @Override
      public boolean validate(TSocket o)
      {
        boolean isClosed = o.getSocket().isClosed();
        log.debug("Is valid object: " + isClosed);
        return !isClosed;
      }
    };

    pool = new ObjectPool(poolConfig, factory);
    pool.registerHost(LOCALHOST, CacheConfig.getServerSocketTimeout(conf), CacheConfig.getServerConnectTimeout(conf));
  }

  public BookKeeperFactory()
  {
  }

  public BookKeeperFactory(BookKeeperService.Iface bookKeeper)
  {
    if (bookKeeper != null) {
      this.bookKeeper = bookKeeper;
    }
  }

  public RetryingPooledBookkeeperClient createBookKeeperClient(String host, Configuration conf) throws TTransportException
  {
    if (!initFlag.get()) {
      synchronized (initFlag) {
        if (!initFlag.get()) {
          init(conf);
          initFlag.set(true);
        }
      }
    }

    if (bookKeeper != null) {
      return new LocalBookKeeperClient(new Poolable<TTransport>(null, null, null), bookKeeper);
    }
    else {
      Poolable<TTransport> obj;
      obj = pool.borrowObject(host, conf);
      RetryingPooledBookkeeperClient retryingBookkeeperClient = new RetryingPooledBookkeeperClient(obj, CacheConfig.getMaxRetries(conf));
      return retryingBookkeeperClient;
    }
  }

  public RetryingPooledBookkeeperClient createBookKeeperClient(String host, Configuration conf, int maxRetries,
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

  public RetryingPooledBookkeeperClient createBookKeeperClient(Configuration conf) throws TTransportException
  {
    return createBookKeeperClient(LOCALHOST, conf);
  }
}
