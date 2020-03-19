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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * @author Daniel
 */
public class ObjectPool<T>
{
  private final PoolConfig config;
  private final ObjectFactory<T> factory;
  private final ObjectPoolPartition<T>[] partitions;
  private Scavenger scavenger;
  private volatile boolean shuttingDown;

  public ObjectPool(PoolConfig poolConfig, ObjectFactory<T> objectFactory)
  {
    this.config = poolConfig;
    this.factory = objectFactory;
    this.partitions = new ObjectPoolPartition[config.getPartitionSize()];
    if (config.getScavengeIntervalMilliseconds() > 0) {
      this.scavenger = new Scavenger();
      this.scavenger.start();
    }
  }

  public void registerHost(String host, int socketTimeout, int connectTimeout, int index)
  {
    partitions[index] = new ObjectPoolPartition<>(this, index, config, factory, createBlockingQueue(config), host, socketTimeout, connectTimeout);
  }

  protected BlockingQueue<Poolable<T>> createBlockingQueue(PoolConfig poolConfig)
  {
    return new ArrayBlockingQueue<>(poolConfig.getMaxSize());
  }

  public Poolable<T> borrowObject(int partitionNumber)
          throws SocketException
  {
    return borrowObject(true, partitionNumber);
  }

  public Poolable<T> borrowObject(boolean blocking, int partitionNumber)
          throws SocketException
  {
    Log.info("Borrowing object for partition: ", partitionNumber);
    for (int i = 0; i < 3; i++) { // try at most three times
      Poolable<T> result = getObject(blocking, partitionNumber);
      if (factory.validate(result.getObject())) {
        return result;
      }
      else {
        this.partitions[result.getPartition()].decreaseObject(result);
      }
    }
    throw new RuntimeException("Cannot find a valid object");
  }

  private Poolable<T> getObject(boolean blocking, int partitionNumber)
  {
    if (shuttingDown) {
      throw new IllegalStateException("Your pool is shutting down");
    }
    int partition = partitionNumber % this.config.getPartitionSize();
    ObjectPoolPartition<T> subPool = this.partitions[partition];

    Poolable<T> freeObject;
    if (subPool.getObjectQueue().size() == 0) {
      // increase objects and return one, it will return null if reach max size
      subPool.increaseObjects(this.config.getDelta());
      try {
        if (blocking) {
          Log.warn("Pool is empty...Waiting for connection");
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
    }
    else {
      freeObject = subPool.getObjectQueue().poll();
    }
    freeObject.setLastAccessTs(System.currentTimeMillis());
    return freeObject;
  }

  public void returnObject(Poolable<T> obj)
  {
    ObjectPoolPartition<T> subPool = this.partitions[obj.getPartition()];
    try {
      Log.debug("Returning object to queue. Queue size: ", subPool.getObjectQueue().size(), "partition id: ", obj.getPartition());
      subPool.getObjectQueue().put(obj);
    }
    catch (InterruptedException e) {
      throw new RuntimeException(e); // impossible for now, unless there is a bug, e,g. borrow once but return twice.
    }
  }

  public int getSize()
  {
    int size = 0;
    for (ObjectPoolPartition<T> subPool : partitions) {
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
    for (ObjectPoolPartition<T> partition : partitions) {
      removed += partition.shutdown();
    }
    return removed;
  }

  private class Scavenger
          extends Thread
  {
    @Override
    public void run()
    {
      int partition = 0;
      while (!ObjectPool.this.shuttingDown) {
        try {
          Thread.sleep(config.getScavengeIntervalMilliseconds());
          partition = ++partition % config.getPartitionSize();
          Log.debug("scavenge sub pool ", partition);
          partitions[partition].scavenge();
        }
        catch (InterruptedException ignored) {
        }
        catch (SocketException e) {
          e.printStackTrace();
        }
      }
    }
  }
}
