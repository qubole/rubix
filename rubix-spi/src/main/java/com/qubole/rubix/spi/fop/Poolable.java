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

/**
 * @author Daniel
 * AutoCloseable.close() is not idemponent, so don't close it multiple times!
 */
public class Poolable<T>
        implements AutoCloseable
{
  private final T object;
  private ObjectPool<T> pool;
  private final String host;
  private long lastAccessTs;

  public Poolable(T t, ObjectPool<T> pool, String host)
  {
    this.object = t;
    this.pool = pool;
    this.host = host;
    this.lastAccessTs = System.currentTimeMillis();
  }

  public T getObject()
  {
    return object;
  }

  public ObjectPool<T> getPool()
  {
    return pool;
  }

  public String getHost()
  {
    return host;
  }

  public void returnObject()
  {
    pool.returnObject(this);
  }

  public long getLastAccessTs()
  {
    return lastAccessTs;
  }

  public void setLastAccessTs(long lastAccessTs)
  {
    this.lastAccessTs = lastAccessTs;
  }

  /**
   * This method is not idemponent, don't call it twice, which will return the object twice to the pool and cause severe problems.
   */
  @Override
  public void close()
  {
    this.returnObject();
  }
}
