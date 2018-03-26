/**
 * Copyright (c) 2016. Qubole Inc
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
package com.qubole.rubix.core;

/**
 * Created by stagra on 28/1/16.
 */
public class ReadRequestChainStats
{
  // All data below in MB
  // From Remote
  private long prefixRead;
  private long suffixRead;
  private long requestedRead;
  private long warmupPenalty;
  private long remoteReads;

  // From cached
  private long cachedDataRead;
  private long cachedReads;

  private long nonLocalReads;
  private long nonLocalDataRead;

  public long getPrefixRead()
  {
    return prefixRead;
  }

  public ReadRequestChainStats setPrefixRead(long prefixReadInBytes)
  {
    this.prefixRead = prefixReadInBytes;
    return this;
  }

  public long getSuffixRead()
  {
    return suffixRead;
  }

  public ReadRequestChainStats setSuffixRead(long suffixRead)
  {
    this.suffixRead = suffixRead;
    return this;
  }

  public long getRequestedRead()
  {
    return requestedRead;
  }

  public ReadRequestChainStats setRequestedRead(long requestedRead)
  {
    this.requestedRead = requestedRead;
    return this;
  }

  public long getTotalDownloaded()
  {
    return prefixRead + requestedRead + suffixRead;
  }

  public long getExtraRead()
  {
    return prefixRead + suffixRead;
  }

  public ReadRequestChainStats setRemoteReads(long remoteReads)
  {
    this.remoteReads = remoteReads;
    return this;
  }

  public long getRemoteReads()
  {
    return remoteReads;
  }

  public long getWarmupPenalty()
  {
    return warmupPenalty;
  }

  public ReadRequestChainStats setWarmupPenalty(long warmupPenalty)
  {
    this.warmupPenalty = warmupPenalty;
    return this;
  }

  public long getCachedDataRead()
  {
    return cachedDataRead;
  }

  public ReadRequestChainStats setCachedDataRead(long cachedDataRead)
  {
    this.cachedDataRead = cachedDataRead;
    return this;
  }

  public ReadRequestChainStats setCachedReads(long cachedReads)
  {
    this.cachedReads = cachedReads;
    return this;
  }

  public long getCachedReads()
  {
    return cachedReads;
  }

  public ReadRequestChainStats setNonLocalReads(long nonLocalReads)
  {
    this.nonLocalReads = nonLocalReads;
    return this;
  }

  public long getNonLocalReads()
  {
    return nonLocalReads;
  }

  public ReadRequestChainStats setNonLocalDataRead(long nonLocalDataRead)
  {
    this.nonLocalDataRead = nonLocalDataRead;
    return this;
  }

  public long getNonLocalDataRead()
  {
    return nonLocalDataRead;
  }

  public ReadRequestChainStats add(ReadRequestChainStats other)
  {
    return new ReadRequestChainStats()
        .setCachedDataRead(cachedDataRead + other.getCachedDataRead())
        .setPrefixRead(prefixRead + other.getPrefixRead())
        .setRequestedRead(requestedRead + other.getRequestedRead())
        .setSuffixRead(suffixRead + other.getSuffixRead())
        .setRemoteReads(remoteReads + other.getRemoteReads())
        .setWarmupPenalty(warmupPenalty + other.getWarmupPenalty())
        .setCachedReads(cachedReads + other.getCachedReads())
        .setNonLocalReads(nonLocalReads + other.getNonLocalReads())
        .setNonLocalDataRead(nonLocalDataRead + other.getNonLocalDataRead());
  }
}
