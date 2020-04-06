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

import com.qubole.rubix.spi.RetryingPooledBookkeeperClient;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.concurrent.BlockingQueue;

/**
 * @author Daniel
 */
public class ObjectPoolPartition<T>
{
  private static final Log log = LogFactory.getLog(RetryingPooledBookkeeperClient.class);

  private final ObjectPool<T> pool;
  private final PoolConfig config;
  private final BlockingQueue<Poolable<T>> objectQueue;
  private final ObjectFactory<T> objectFactory;
  private int totalCount;
  private String host;
  private int socketTimeout;
  private int connectTimeout;

  public ObjectPoolPartition(ObjectPool<T> pool, PoolConfig config,
          ObjectFactory<T> objectFactory, BlockingQueue<Poolable<T>> queue, String host, int socketTimeout, int connectTimeout)
  {
    this.pool = pool;
    this.config = config;
    this.objectFactory = objectFactory;
    this.objectQueue = queue;
    this.host = host;
    this.socketTimeout = socketTimeout;
    this.connectTimeout = connectTimeout;
    for (int i = 0; i < config.getMinSize(); i++) {
      objectQueue.add(new Poolable<>(objectFactory.create(host, socketTimeout, connectTimeout), pool, host));
    }
    totalCount = config.getMinSize();
  }

  public BlockingQueue<Poolable<T>> getObjectQueue()
  {
    return objectQueue;
  }

  public synchronized int increaseObjects(int delta)
  {
    if (delta + totalCount > config.getMaxSize()) {
      delta = config.getMaxSize() - totalCount;
    }
    try {
      for (int i = 0; i < delta; i++) {
        objectQueue.put(new Poolable<>(objectFactory.create(host, socketTimeout, connectTimeout), pool, host));
      }
      totalCount += delta;
      log.debug("Increased pool size to: " + totalCount + ", current queue size: " + objectQueue.size());
    }
    catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    return delta;
  }

  public synchronized boolean decreaseObject(Poolable<T> obj)
  {
    objectFactory.destroy(obj.getObject());
    totalCount--;
    return true;
  }

  public synchronized int getTotalCount()
  {
    return totalCount;
  }
}
