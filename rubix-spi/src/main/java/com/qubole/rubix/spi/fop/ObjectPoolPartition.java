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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkState;

/**
 * @author Daniel
 */
public class ObjectPoolPartition<T>
{
  private final CustomLogger log;
  private final ObjectPool<T> pool;
  private final PoolConfig config;
  private final BlockingQueue<Poolable<T>> objectQueue;
  private final ObjectFactory<T> objectFactory;
  private final String host;
  private final int socketTimeout;
  private final int connectTimeout;
  private final Semaphore takeSemaphore;
  private final AtomicInteger aliveObjectCount;

  public ObjectPoolPartition(ObjectPool<T> pool, PoolConfig config,
          ObjectFactory<T> objectFactory, BlockingQueue<Poolable<T>> queue, String host, String name)
  {
    this.pool = pool;
    this.config = config;
    this.objectFactory = objectFactory;
    this.objectQueue = queue;
    this.host = host;
    this.socketTimeout = config.getSocketTimeoutMilliseconds();
    this.connectTimeout = config.getConnectTimeoutMilliseconds();
    this.aliveObjectCount = new AtomicInteger();
    this.log = new CustomLogger(name, host);
    this.takeSemaphore = new Semaphore(config.getMaxSize(), true);
    try {
      for (int i = 0; i < config.getMinSize(); i++) {
          T object = objectFactory.create(host, socketTimeout, connectTimeout);
          objectQueue.add(new Poolable<>(object, pool, host));
          aliveObjectCount.incrementAndGet();
      }
    }
    catch (Exception e) {
      // skipping logging the exception as factories are already logging.
    }
  }

  public void returnObject(Poolable<T> object)
  {
    try {
      if (!objectFactory.validate(object.getObject())) {
        log.debug(String.format("Invalid object...removing: %s ", object));
        decreaseObject(object);
        return;
      }

      log.debug(String.format("Returning object: %s to queue. Queue size: %d", object, objectQueue.size()));
      if (!objectQueue.offer(object)) {
        String errorLog = "Created more objects than configured. Created=" + aliveObjectCount + " QueueSize=" + objectQueue.size();
        log.warn(errorLog);
        decreaseObject(object);
        throw new RuntimeException(errorLog);
      }
    }
    finally {
      takeSemaphore.release();
    }
  }

  public boolean decreaseObject(Poolable<T> obj)
  {
    checkState(obj.getHost() != null, "Invalid object");
    checkState(obj.getHost().equals(this.host),
            "Call to free object of wrong partition, current partition=%s requested partition = %s",
            this.host, obj.getHost());
    log.debug("Decreasing pool size object: " + obj);
    objectFactory.destroy(obj.getObject());
    aliveObjectCount.decrementAndGet();
    obj.destroy();
    return true;
  }

  public Poolable<T> getObject()
  {
    Poolable<T> object;
    try {
      if (!takeSemaphore.tryAcquire(config.getMaxWaitMilliseconds(), TimeUnit.MILLISECONDS)) {
        // Not able to acquire semaphore in the given timeout, return null
        return null;
      }
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return null;
    }

    try {
        object = tryGetObject();
        object.setLastAccessTs(System.currentTimeMillis());
    }
    catch (Exception e) {
      takeSemaphore.release();
      throw new RuntimeException("Cannot get a free object from the pool", e);
    }
    return object;
  }

  private Poolable<T> tryGetObject() throws Exception
  {
    Poolable<T> poolable = objectQueue.poll();
    if (poolable == null)
    {
      try {
        T object = objectFactory.create(host, socketTimeout, connectTimeout);
        poolable = new Poolable<>(object, pool, host);
        aliveObjectCount.incrementAndGet();
        log.debug(String.format("Added a connection, Pool state: totalCount: %s, queueSize: %d", aliveObjectCount,
                objectQueue.size()));
      }
      catch (Exception e) {
        log.warn(String.format("Unable create a connection. Pool state: totalCount=%s queueSize=%d", aliveObjectCount,
                objectQueue.size()), e);
        if (poolable != null) {
          objectFactory.destroy(poolable.getObject());
          poolable.destroy();
        }
        throw e;
      }
    }
    return poolable;
  }

  public int getAliveObjectCount()
  {
    return aliveObjectCount.get();
  }

  // set the scavenge interval carefully
  public void scavenge() throws InterruptedException
  {
    int delta = this.aliveObjectCount.get() - config.getMinSize();
    if (delta <= 0) {
      log.debug("Scavenge for delta <= 0, Skipping !!!");
      return;
    }
    int removed = 0;
    long now = System.currentTimeMillis();
    Poolable<T> obj;
    while (delta-- > 0) {
      obj = objectQueue.poll();
      if (obj == null) {
        break;
      }
      // performance trade off: delta always decrease even if the queue is empty,
      // so it could take several intervals to shrink the pool to the configured min value.
      log.debug(String.format("obj=%s, now-last=%s, max idle=%s", obj, now - obj.getLastAccessTs(),
              config.getMaxIdleMilliseconds()));
      if (now - obj.getLastAccessTs() > config.getMaxIdleMilliseconds() &&
              ThreadLocalRandom.current().nextDouble(1) < config.getScavengeRatio()) {
        log.debug("Scavenger removing object: " + obj);
        decreaseObject(obj); // shrink the pool size if the object reaches max idle time
        removed++;
      }
      else {
        objectQueue.put(obj); //put it back
        // breaking the loop assuming connections will be in order of their access time in the queue
        // i.e. older -> new
        break;
      }
    }
    if (removed > 0) {
      log.debug(removed + " objects were scavenged");
    }
  }

  public synchronized int shutdown()
  {
    int removed = 0;
    while (this.aliveObjectCount.get() > 0) {
      Poolable<T> obj = objectQueue.poll();
      if (obj != null) {
        decreaseObject(obj);
        removed++;
      }
    }
    return removed;
  }

  public String getHost()
  {
    return host;
  }

  private class CustomLogger {
    private static final String logFormatStr = "Pool: %s : Host: %s : %s";

    private final Log log = LogFactory.getLog(ObjectPoolPartition.class);
    private final String poolName;
    private final String hostName;

    public CustomLogger(String poolName, String host)
    {
      this.poolName = poolName;
      this.hostName = host;
    }

    public void info(String message)
    {
      log.info(getLogMessage(message));
    }

    public void debug(String message)
    {
      log.debug(getLogMessage(message));
    }

    public void warn(String message)
    {
      log.warn(getLogMessage(message));
    }

    public void warn(String message, Throwable t)
    {
      log.warn(getLogMessage(message), t);
    }

    private String getLogMessage(String message)
    {
      return String.format(logFormatStr, poolName, hostName, message);
    }
  }
}
