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
 */
package com.qubole.rubix.bookkeeper;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheStats;
import com.google.common.collect.ImmutableMap;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;

public class ThrowingEmptyCache<K, V> implements Cache<K, V>
{
  public static final CacheStats STATS = new CacheStats(
          0,
          0,
          0,
          0,
          0,
          0);

  @Nullable
  @Override
  public V getIfPresent(Object key)
  {
    throw new UnsupportedOperationException("accessing contents is not supported");
  }

  @Override
  public V get(K key, Callable<? extends V> loader)
          throws ExecutionException
  {
    throw new UnsupportedOperationException("accessing contents is not supported");
  }

  @Override
  public ImmutableMap<K, V> getAllPresent(Iterable<?> keys)
  {
    throw new UnsupportedOperationException("accessing contents is not supported");
  }

  @Override
  public void put(K key, V value)
  {
    throw new UnsupportedOperationException("accessing contents is not supported");
  }

  @Override
  public void putAll(Map<? extends K, ? extends V> m)
  {
    if (!m.isEmpty()) {
      throw new UnsupportedOperationException("accessing contents is not supported");
    }
  }

  @Override
  public void invalidate(Object key)
  {
    throw new UnsupportedOperationException("accessing contents is not supported");
  }

  @Override
  public void invalidateAll(Iterable<?> keys)
  {
    throw new UnsupportedOperationException("accessing contents is not supported");
  }

  @Override
  public void invalidateAll()
  {
    throw new UnsupportedOperationException("accessing contents is not supported");
  }

  @Override
  public long size()
  {
    return 0;
  }

  @Override
  public CacheStats stats()
  {
    return STATS;
  }

  @Override
  public ConcurrentMap<K, V> asMap()
  {
    throw new UnsupportedOperationException("accessing contents is not supported");
  }

  @Override
  public void cleanUp()
  {
  }
}
