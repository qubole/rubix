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
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkState;

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
    totalCount = 0;
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
      log.debug(String.format("Invalid object for host %s removing %s ", object.getHost(), object));
      decreaseObject(object);
      // Compensate for the removed object. Needed to prevent endless wait when in parallel a borrowObject is called
      increaseObjects(1, false);
      return;
    }

    log.debug(String.format("Returning object %s to queue of host %s. Queue size: %d", object, object.getHost(), objectQueue.size()));
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
}
