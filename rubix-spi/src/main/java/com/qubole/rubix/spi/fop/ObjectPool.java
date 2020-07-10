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

import com.google.common.util.concurrent.AbstractScheduledService;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static java.lang.Thread.currentThread;

/**
 * @author Daniel
 */
public class ObjectPool<T>
{
  private static final Log log = LogFactory.getLog(ObjectPool.class);

  private final PoolConfig config;
  private final ObjectFactory<T> factory;
  private final ConcurrentHashMap<String, ObjectPoolPartition<T>> hostToPoolMap;
  private final String name;
  private Scavenger scavenger;
  private volatile boolean shuttingDown;

  public ObjectPool(PoolConfig poolConfig, ObjectFactory<T> objectFactory, String name)
  {
    this.config = poolConfig;
    this.factory = objectFactory;
    this.hostToPoolMap = new ConcurrentHashMap<>();
    this.name = name;
    if (config.getScavengeIntervalMilliseconds() > 0) {
      this.scavenger = new Scavenger(name);
      this.scavenger.startAsync();
    }
  }

  public void registerHost(String host)
  {
    hostToPoolMap.put(host, new ObjectPoolPartition<>(this, config, factory, createBlockingQueue(config), host, this.name));
  }

  protected BlockingQueue<Poolable<T>> createBlockingQueue(PoolConfig poolConfig)
  {
    return new ArrayBlockingQueue<>(poolConfig.getMaxSize());
  }

  public Poolable<T> borrowObject(String host)
  {
    if (!hostToPoolMap.containsKey(host)) {
      synchronized (hostToPoolMap) {
        if (!hostToPoolMap.containsKey(host)) {
          registerHost(host);
        }
      }
    }
    log.debug(this.name + " : Borrowing object for partition: " + host);
    for (int i = 0; i < 3; i++) { // try at most three times
      Poolable<T> result = getObject(host);
      if (result == null) {
        continue;
      }
      else if (factory.validate(result.getObject())) {
        return result;
      }
      else {
        this.hostToPoolMap.get(host).decreaseObject(result);
      }
    }
    throw new RuntimeException("Cannot find a valid object");
  }

  private Poolable<T> getObject(String host)
  {
    ObjectPoolPartition<T> subPool = this.hostToPoolMap.get(host);
    return subPool.getObject();
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
      size += subPool.getAliveObjectCount();
    }
    return size;
  }

  public synchronized int shutdown()
          throws InterruptedException
  {
    shuttingDown = true;
    int removed = 0;
    if (scavenger != null) {
      scavenger.stopAsync();
    }
    for (ObjectPoolPartition<T> subPool : hostToPoolMap.values()) {
      removed += subPool.shutdown();
    }
    return removed;
  }

  private class Scavenger
          extends AbstractScheduledService
  {
    private final String poolName;

    public Scavenger(String poolName)
    {
      this.poolName = poolName;
    }

    @Override
    protected ScheduledExecutorService executor() {
      return Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = Executors.defaultThreadFactory().newThread(r);
        t.setName("scavenger-" + poolName);
        t.setDaemon(true);
        return t;
      });
    }

    @Override
    protected Scheduler scheduler()
    {
      int delay = config.getScavengeIntervalMilliseconds();
      log.debug("Starting scavenger for connection pool with delay: " + delay + " ms");
      return Scheduler.newFixedDelaySchedule(delay, delay, TimeUnit.MILLISECONDS);
    }

    @Override
    protected void runOneIteration()
    {
      if (ObjectPool.this.shuttingDown) {
        log.debug(name + " : Pool is shutting down, skip scavenger");
        return;
      }
      try {
        log.debug(name + " : Host pool map values: " + hostToPoolMap.values());
        for (ObjectPoolPartition<T> subPool : hostToPoolMap.values()) {
          log.debug(name + " : Scavenging sub pool of host: " + subPool.getHost());
          subPool.scavenge();
        }
      }
      catch (InterruptedException e) {
        log.warn(name + " : Scavenge failed with error", e);
        currentThread().interrupt();
      }
    }
  }

}
