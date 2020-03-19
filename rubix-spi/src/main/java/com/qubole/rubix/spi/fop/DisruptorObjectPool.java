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

import com.conversantmedia.util.concurrent.DisruptorBlockingQueue;

import java.util.concurrent.BlockingQueue;

public class DisruptorObjectPool<T>
        extends ObjectPool<T>
{
  public DisruptorObjectPool(PoolConfig poolConfig, ObjectFactory<T> objectFactory)
  {
    super(poolConfig, objectFactory);
  }

  @Override
  protected BlockingQueue<Poolable<T>> createBlockingQueue(PoolConfig config)
  {
    return new DisruptorBlockingQueue<>(config.getMaxSize());
  }
}
