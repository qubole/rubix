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
 * <p>
 * <p>
 * NOTICE: THIS FILE HAS BEEN MODIFIED BY  Qubole Inc UNDER COMPLIANCE WITH THE APACHE 2.0 LICENCE FROM THE ORIGINAL WORK
 * OF https://github.com/DanielYWoo/fast-object-pool.
 */
package com.qubole.rubix.spi.fop;

import com.qubole.rubix.spi.CacheConfig;
import com.qubole.rubix.spi.RetryingPooledBookkeeperClient;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Daniel
 */
public class ObjectPool<T>
{
  private static final Log log = LogFactory.getLog(RetryingPooledBookkeeperClient.class);

  private final PoolConfig config;
  private final ObjectFactory<T> factory;
  private final ConcurrentHashMap<String, ObjectPoolPartition<T>> hostToPoolMap;
  private Scavenger scavenger;
  private volatile boolean shuttingDown;

  public ObjectPool(PoolConfig poolConfig, ObjectFactory<T> objectFactory)
  {
    this.config = poolConfig;
    this.factory = objectFactory;
    this.hostToPoolMap = new ConcurrentHashMap<>();
    if (config.getScavengeIntervalMilliseconds() > 0) {
      this.scavenger = new Scavenger();
      this.scavenger.start();
    }
  }

  public void registerHost(String host, int socketTimeout, int connectTimeout)
  {
    hostToPoolMap.put(host, new ObjectPoolPartition<>(this, config, factory, createBlockingQueue(config), host, socketTimeout, connectTimeout));
  }

  protected BlockingQueue<Poolable<T>> createBlockingQueue(PoolConfig poolConfig)
  {
    return new ArrayBlockingQueue<>(poolConfig.getMaxSize());
  }

  public Poolable<T> borrowObject(String host, Configuration conf)
  {
    if (!hostToPoolMap.containsKey(host)) {
      synchronized (hostToPoolMap) {
        if (!hostToPoolMap.containsKey(host)) {
          int socketTimeout = CacheConfig.getServerSocketTimeout(conf);
          int connectTimeout = CacheConfig.getServerConnectTimeout(conf);
          registerHost(host, socketTimeout, connectTimeout);
        }
      }
    }
    log.debug("Borrowing object for partition: " + host);
    for (int i = 0; i < 3; i++) { // try at most three times
      Poolable<T> result = getObject(false, host);
      if (factory.validate(result.getObject())) {
        return result;
      }
      else {
        this.hostToPoolMap.get(host).decreaseObject(result);
      }
    }
    throw new RuntimeException("Cannot find a valid object");
  }

  private Poolable<T> getObject(boolean blocking, String host)
  {
    ObjectPoolPartition<T> subPool = this.hostToPoolMap.get(host);
    return subPool.getObject(blocking);
  }

  public void returnObject(Poolable<T> obj)
  {
    ObjectPoolPartition<T> subPool = this.hostToPoolMap.get(obj.getHost());
    subPool.returnObject(obj);
  }

  public int getSize()
  {
    int size = 0;
    for (ObjectPoolPartition<T> subPool : hostToPoolMap.values()) {
      size += subPool.getTotalCount();
    }
    return size;
  }

  public synchronized int shutdown()
          throws InterruptedException
  {
    shuttingDown = true;
    int removed = 0;
    if (scavenger != null) {
      scavenger.interrupt();
      scavenger.join();
    }
    for (ObjectPoolPartition<T> subPool : hostToPoolMap.values()) {
      removed += subPool.shutdown();
    }
    return removed;
  }

  private class Scavenger
          extends Thread
  {
    @Override
    public void run()
    {
      while (!ObjectPool.this.shuttingDown) {
        try {
          for (ObjectPoolPartition<T> subPool : hostToPoolMap.values()) {
            Thread.sleep(config.getScavengeIntervalMilliseconds());
            log.debug("scavenge sub pool of host" + subPool.getHost());
            subPool.scavenge();
          }
        }
        catch (InterruptedException e) {
          log.debug("scavenge for sub pool failed with error", e);
        }
      }
    }
  }
}
