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

import java.net.SocketException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by sakshia on 5/10/16.
 */
public class BookKeeperFactory
{
  BookKeeperService.Iface bookKeeper;
  private static Log log = LogFactory.getLog(BookKeeperFactory.class.getName());
  private static final AtomicInteger count = new AtomicInteger();
  private static final AtomicBoolean flag = new AtomicBoolean();
  private static PoolConfig poolConfig;
  private static ConcurrentHashMap<String, Integer> concurrentHashMap = new ConcurrentHashMap<>();
  private static ObjectFactory<TSocket> factory;
  static ObjectPool pool;

  private static final String LOCALHOST = "localhost";

  static {
    // TODO: add config.properties file to set this config
    poolConfig = new PoolConfig();
    poolConfig.setPartitionSize(10);
    poolConfig.setMaxSize(2000);
    poolConfig.setMinSize(100);
    poolConfig.setDelta(100);
    poolConfig.setMaxIdleMilliseconds(60 * 1000 * 5);
    final int socketTimeout = 60000;
    final int connectTimeout = 10000;

    factory = new ObjectFactory<TSocket>()
    {
      @Override
      public TSocket create(String host, int socketTimeout, int connectTimeout)
      {
        log.debug("Opening connection on host: " + host);
        TSocket socket = null;
        try {
          socket = new TSocket(host, 8899, socketTimeout, connectTimeout); // create your object here
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
        return !isClosed; // validate your object here
      }
    };

    if (!flag.get()) {
      synchronized (flag) {
        if (!flag.get()) {
          pool = new ObjectPool(poolConfig, factory);
          log.debug("Registering host: localhost count: " + count.get());
          int index = count.getAndAdd(1);
          pool.registerHost(LOCALHOST, socketTimeout, connectTimeout, index);
          concurrentHashMap.put(LOCALHOST, index);
          flag.set(true);
        }
      }
    }
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

  public RetryingBookkeeperClient createBookKeeperClient(String host, Configuration conf) throws TTransportException
  {
    if (bookKeeper != null) {
      return new LocalBookKeeperClient(null, bookKeeper);
    }
    else {
      if (!concurrentHashMap.containsKey(host)) {
        synchronized (concurrentHashMap) {
          if (!concurrentHashMap.containsKey(host)) {
            final int socketTimeout = CacheConfig.getServerSocketTimeout(conf);
            final int connectTimeout = CacheConfig.getServerConnectTimeout(conf);
            log.info("Registering host on connection pool, hostname: " + host + " pool ID: " + count.get());
            int index = count.getAndAdd(1);
            pool.registerHost(host, socketTimeout, connectTimeout, index);
            concurrentHashMap.put(host, index);
          }
        }
      }

      Poolable<TTransport> obj;
      try {
        obj = pool.borrowObject(concurrentHashMap.get(host));
      }
      catch (SocketException e) {
        e.printStackTrace();
        throw new TTransportException();
      }
      RetryingBookkeeperClient retryingBookkeeperClient = new RetryingBookkeeperClient(obj, CacheConfig.getMaxRetries(conf));
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
    return createBookKeeperClient(LOCALHOST, conf);
  }

  public void returnBookKeeperClient(Poolable<TTransport> obj)
  {
    if (obj != null && obj.getObject() != null && obj.getObject().isOpen()) {
      pool.returnObject(obj);
    }
  }
}
