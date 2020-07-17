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
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

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
  private int totalCount;
  private final String host;
  private final int socketTimeout;
  private final int connectTimeout;

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
    this.totalCount = 0;
    this.log = new CustomLogger(name, host);
    for (int i = 0; i < config.getMinSize(); i++) {
      T object = objectFactory.create(host, socketTimeout, connectTimeout);
      if (object != null) {
        objectQueue.add(new Poolable<>(object, pool, host));
        totalCount++;
      }
    }
  }

  public void returnObject(Poolable<T> object)
  {
    if (!objectFactory.validate(object.getObject())) {
      log.debug(String.format("Invalid object...removing: %s ", object));
      decreaseObject(object);
      // Compensate for the removed object. Needed to prevent endless wait when in parallel a borrowObject is called
      increaseObjects(1, false);
      return;
    }

    log.debug(String.format("Returning object: %s to queue. Queue size: %d", object, objectQueue.size()));
    if (!objectQueue.offer(object)) {
      log.warn("Created more objects than configured. Created=" + totalCount + " QueueSize=" + objectQueue.size());
      decreaseObject(object);
    }
  }

  public Poolable<T> getObject(boolean blocking)
  {
    if (objectQueue.size() == 0) {
      // increase objects and return one, it will return null if pool reaches max size or if object creation fails
      Poolable<T> object = increaseObjects(this.config.getDelta(), true);

      if (object != null) {
        return object;
      }

      if (totalCount == 0) {
        // Could not create objects, this is mostly due to connection timeouts hence no point blocking as there is not other producer of sockets
        throw new RuntimeException("Could not add connections to pool");
      }
      // else wait for a connection to get free
    }

    Poolable<T> freeObject;
    try {
      if (blocking) {
        freeObject = objectQueue.take();
      }
      else {
        freeObject = objectQueue.poll(config.getMaxWaitMilliseconds(), TimeUnit.MILLISECONDS);
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

  private synchronized Poolable<T> increaseObjects(int delta, boolean returnObject)
  {
    int oldCount = totalCount;
    if (delta + totalCount > config.getMaxSize()) {
      delta = config.getMaxSize() - totalCount;
    }

    Poolable<T> objectToReturn = null;
    try {
      for (int i = 0; i < delta; i++) {
        T object = objectFactory.create(host, socketTimeout, connectTimeout);
        if (object != null) {
          // Do not put the first object on queue
          // it will be returned to the caller to ensure it's request is satisfied first if object is requested
          Poolable<T> poolable = new Poolable<>(object, pool, host);
          if (objectToReturn == null && returnObject) {
            objectToReturn = poolable;
          }
          else {
            objectQueue.put(poolable);
          }
          totalCount++;
        }
      }

      if (delta > 0 && (totalCount - oldCount) == 0) {
        log.warn(String.format("Could not increase pool size. Pool state: totalCount=%d queueSize=%d delta=%d", totalCount, objectQueue.size(), delta));
      }
      else {
        log.debug(String.format("Increased pool size by %d, to new size: %d, current queue size: %d, delta: %d",
                totalCount - oldCount, totalCount, objectQueue.size(), delta));
      }
    }
    catch (Exception e) {
      log.warn(String.format("Unable to increase pool size. Pool state: totalCount=%d queueSize=%d delta=%d", totalCount, objectQueue.size(), delta), e);
      // objectToReturn is not on the queue hence untracked, clean it up before forwarding exception
      if (objectToReturn != null) {
        objectFactory.destroy(objectToReturn.getObject());
        objectToReturn.destroy();
      }
      throw new RuntimeException(e);
    }

    return objectToReturn;
  }

  public boolean decreaseObject(Poolable<T> obj)
  {
    checkState(obj.getHost() != null, "Invalid object");
    checkState(obj.getHost().equals(this.host),
            "Call to free object of wrong partition, current partition=%s requested partition = %s",
            this.host, obj.getHost());
    objectRemoved();
    log.debug("Decreasing pool size object: " + obj);
    objectFactory.destroy(obj.getObject());
    obj.destroy();
    return true;
  }

  private synchronized void objectRemoved()
  {
    totalCount--;
  }

  public synchronized int getTotalCount()
  {
    return totalCount;
  }

  // set the scavenge interval carefully
  public void scavenge() throws InterruptedException
  {
    int delta = this.totalCount - config.getMinSize();
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
    while (this.totalCount > 0) {
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
