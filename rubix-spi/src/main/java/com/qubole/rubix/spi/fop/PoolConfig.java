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
 */
public class PoolConfig
{
  private int maxWaitMilliseconds = 5000; // when pool is full, wait at most 5 seconds, then throw an exception
  private int minSize = 5;
  private int maxSize = 20;
  private int delta = 5;

  public int getMaxWaitMilliseconds()
  {
    return maxWaitMilliseconds;
  }

  public PoolConfig setMaxWaitMilliseconds(int maxWaitMilliseconds)
  {
    this.maxWaitMilliseconds = maxWaitMilliseconds;
    return this;
  }

  public int getMinSize()
  {
    return minSize;
  }

  public PoolConfig setMinSize(int minSize)
  {
    this.minSize = minSize;
    return this;
  }

  public int getMaxSize()
  {
    return maxSize;
  }

  public PoolConfig setMaxSize(int maxSize)
  {
    this.maxSize = maxSize;
    return this;
  }

  public int getDelta()
  {
    return delta;
  }

  public void setDelta(int delta)
  {
    this.delta = delta;
  }
}
