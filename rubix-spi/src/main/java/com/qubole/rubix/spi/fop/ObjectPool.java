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
import java.util.concurrent.TimeUnit;

/**
 * @author Daniel
 */
public class ObjectPool<T>
{
  private static final Log log = LogFactory.getLog(RetryingPooledBookkeeperClient.class);

  private final PoolConfig config;
  private final ObjectFactory<T> factory;
  private final ConcurrentHashMap<String, ObjectPoolPartition<T>> hostToPoolMap;

  public ObjectPool(PoolConfig poolConfig, ObjectFactory<T> objectFactory)
  {
    this.config = poolConfig;
    this.factory = objectFactory;
    this.hostToPoolMap = new ConcurrentHashMap<>();
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

    Poolable<T> freeObject;
    if (subPool.getObjectQueue().size() == 0) {
      // increase objects and return one, it will return null if reach max size
      int totalObjects = subPool.increaseObjects(this.config.getDelta());
      if (totalObjects == 0) {
        // Could not create objects, this is mostly due to connection timeouts hence no point blocking as there is not other producer of sockets
        throw new RuntimeException("Could not add connections to pool");
      }
    }

    try {
      if (blocking) {
        freeObject = subPool.getObjectQueue().take();
      }
      else {
        freeObject = subPool.getObjectQueue().poll(config.getMaxWaitMilliseconds(), TimeUnit.MILLISECONDS);
        if (freeObject == null) {
          throw new RuntimeException("Cannot get a free object from the pool");
        }
      }
    }
    catch (InterruptedException e) {
      throw new RuntimeException(e); // will never happen
    }

    freeObject.setLastAccessTs(System.currentTimeMillis());
    return freeObject;
  }

  public void returnObject(Poolable<T> obj)
  {
    ObjectPoolPartition<T> subPool = this.hostToPoolMap.get(obj.getHost());
    if (!factory.validate(obj.getObject())) {
      log.debug(String.format("Invalid object for host %s removing %s ", obj.getHost(), obj));
      subPool.decreaseObject(obj);
      // Compensate for the removed object. Needed to prevent endless wait when in parallel a borrowObject is called
      subPool.increaseObjects(1);
      return;
    }

    try {
      log.debug(String.format("Returning object %s to queue of host %s. Queue size: %d", obj, obj.getHost(), subPool.getObjectQueue().size()));
      subPool.getObjectQueue().put(obj);
    }
    catch (InterruptedException e) {
      throw new RuntimeException(e); // impossible for now, unless there is a bug, e,g. borrow once but return twice.
    }
  }

  public int getSize()
  {
    int size = 0;
    for (ObjectPoolPartition<T> subPool : hostToPoolMap.values()) {
      size += subPool.getTotalCount();
    }
    return size;
  }
}
