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

import java.net.SocketException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadLocalRandom;

/**
 * @author Daniel
 */
public class ObjectPoolPartition<T>
{
  private final ObjectPool<T> pool;
  private final PoolConfig config;
  private final int partition;
  private final BlockingQueue<Poolable<T>> objectQueue;
  private final ObjectFactory<T> objectFactory;
  private int totalCount;
  private String host;
  private int socketTimeout;
  private int connectTimeout;

  public ObjectPoolPartition(ObjectPool<T> pool, int partition, PoolConfig config,
          ObjectFactory<T> objectFactory, BlockingQueue<Poolable<T>> queue, String host, int socketTimeout, int connectTimeout)
  {
    this.pool = pool;
    this.config = config;
    this.objectFactory = objectFactory;
    this.partition = partition;
    this.objectQueue = queue;
    this.host = host;
    for (int i = 0; i < config.getMinSize(); i++) {
      objectQueue.add(new Poolable<>(objectFactory.create(host, socketTimeout, connectTimeout), pool, partition));
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
        objectQueue.put(new Poolable<>(objectFactory.create(host, socketTimeout, connectTimeout), pool, partition));
      }
      totalCount += delta;
      Log.info("Increased pool size to: ", totalCount, ", current queue size: ", objectQueue.size());
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

  // set the scavenge interval carefully
  public synchronized void scavenge()
          throws InterruptedException, SocketException
  {
    int delta = this.totalCount - config.getMinSize();
    if (delta <= 0) {
      return;
    }
    int removed = 0;
    long now = System.currentTimeMillis();
    Poolable<T> obj;
    obj = objectQueue.poll();
    while (delta-- > 0 && obj != null) {
      // performance trade off: delta always decrease even if the queue is empty,
      // so it could take several intervals to shrink the pool to the configured min value.
      if (Log.isDebug()) {
        Log.debug("obj=", obj, ", now-last=", now - obj.getLastAccessTs(), ", max idle=",
                config.getMaxIdleMilliseconds());
      }
      if (now - obj.getLastAccessTs() > config.getMaxIdleMilliseconds() &&
              ThreadLocalRandom.current().nextDouble(1) < config.getScavengeRatio()) {
        decreaseObject(obj); // shrink the pool size if the object reaches max idle time
        removed++;
      }
      else {
        objectQueue.put(obj); //put it back
      }
      obj = objectQueue.poll();
    }
    if (removed > 0) {
      Log.debug(removed, " objects were scavenged.");
    }
  }

  public synchronized int shutdown()
  {
    int removed = 0;
    while (this.totalCount > 0) {
      Poolable<T> obj = objectQueue.poll();
      if (obj != null) {
        decreaseObject(obj);
        removed++;
      }
    }
    return removed;
  }
}
