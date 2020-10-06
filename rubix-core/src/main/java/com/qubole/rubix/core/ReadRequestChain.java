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
package com.qubole.rubix.core;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import static com.google.common.base.Preconditions.checkState;

/**
 * Created by stagra on 4/1/16.
 * <p>
 * One ReadRequestChain contains ReadRequests for same buffer
 *
 * ReadRequestChain works with `long` type, even though reads on input streams are limited by the capacity of `int`, in order
 * to be generic and be useful in more cases than just reads on input streams. One such use case is of parallel warmup where
 * reads are accumulated and merged for a period of time which can lead to reads of much bigger size than Integer.MAX_SIZE
 */
public abstract class ReadRequestChain implements Callable<Long>
{
  List<ReadRequest> readRequests = new ArrayList<ReadRequest>();
  ReadRequest lastRequest;
  boolean isLocked;
  boolean cancelled;
  private long maxReadRequestSize;
  protected final int generationNumber;

  protected String threadName;
  protected long requests;

  private static final Log log = LogFactory.getLog(ReadRequestChain.class);

  public ReadRequestChain(int generationNumber)
  {
    this(generationNumber, Long.MAX_VALUE);
  }

  // Caller responsible to keep maxReadRequestSize block aligned
  public ReadRequestChain(int generationNumber, long maxReadRequestSize)
  {
    super();
    this.generationNumber = generationNumber;
    this.maxReadRequestSize = maxReadRequestSize;
    this.threadName = Thread.currentThread().getName();
  }

  @VisibleForTesting
  public void setMaxReadRequestSize(long maxReadRequestSize)
  {
    this.maxReadRequestSize = maxReadRequestSize;
  }

  // Should be added in forward seek fashion for better performance
  public void addReadRequest(ReadRequest readRequest)
  {
    checkState(!isLocked, "Adding request to a locked chain");
    log.debug(String.format("Request to add ReadRequest: [%d, %d, %d, %d, %d]", readRequest.getBackendReadStart(), readRequest.getBackendReadEnd(), readRequest.getActualReadStart(), readRequest.getActualReadEnd(), readRequest.getDestBufferOffset()));
    if (lastRequest == null) {
      lastRequest = readRequest;
    }
    else {
      // since one chain contains request of same buffer, we can collate
      if (lastRequest.getBackendReadEnd() == readRequest.getBackendReadStart()) {
        // Since the ReadRequests coming in are for same buffer, can merge the two
        lastRequest.setBackendReadEnd(readRequest.getBackendReadEnd());
        lastRequest.setActualReadEnd(readRequest.getActualReadEnd());
        log.debug(String.format("Updated last to: [%d, %d, %d, %d, %d]", lastRequest.getBackendReadStart(), lastRequest.getBackendReadEnd(), lastRequest.getActualReadStart(), lastRequest.getActualReadEnd(), lastRequest.getDestBufferOffset()));
      }
      else {
        addRequest(lastRequest);
        lastRequest = readRequest;
      }
    }
    requests++;
  }

  private void addRequest(ReadRequest readRequest)
  {
    long backendReadLength = readRequest.getBackendReadLength();
    if (backendReadLength <= maxReadRequestSize) {
      addRequestToQueue(readRequest);
      return;
    }

    long backendReadStart = 0;
    long actualReadStart = readRequest.getActualReadStart();
    while (backendReadStart < backendReadLength) {
      long backendReadEnd = backendReadStart + Math.min(maxReadRequestSize, backendReadLength - backendReadStart);
      ReadRequest chunkedRequest = new ReadRequest(
              backendReadStart,
              backendReadEnd,
              actualReadStart,
              Math.min(readRequest.getActualReadEnd(), backendReadEnd),
              readRequest.getDestBuffer(),
              readRequest.getDestBufferOffset() + Math.toIntExact(actualReadStart - readRequest.getActualReadStart()),
              readRequest.getBackendFileSize());
      addRequestToQueue(chunkedRequest);
      backendReadStart = backendReadEnd;
      actualReadStart = backendReadStart;
    }
  }

  private void addRequestToQueue(ReadRequest readRequest)
  {
    readRequests.add(readRequest);
    log.debug(String.format("Added ReadRequest: [%d, %d, %d, %d, %d]", readRequest.getBackendReadStart(), readRequest.getBackendReadEnd(), readRequest.getActualReadStart(), readRequest.getActualReadEnd(), readRequest.getDestBufferOffset()));
  }

  public void lock()
  {
    isLocked = true;
    if (lastRequest != null) {
      addRequest(lastRequest);
      lastRequest = null;
    }
  }

  public List<ReadRequest> getReadRequests()
  {
    return readRequests;
  }

  public abstract ReadRequestChainStats getStats();

  /*
   * This method is used called to update the cache status after read has been done
   */
  public void updateCacheStatus(String remotePath, long fileSize, long lastModified, int blockSize, Configuration conf)
  {
    // no-op by default
  }

  public void cancel()
  {
    cancelled = true;
  }

  protected void propagateCancel(String className)
      throws IOException
  {
    throw new CancelledException(className + " Cancelled");
  }

  protected class CancelledException
      extends IOException
  {
    public CancelledException(String message)
    {
      super(message);
    }
  }

  protected class InvalidationRequiredException
    extends IOException
  {
    public InvalidationRequiredException(String message)
    {
      super(message);
    }
  }

  public boolean isCancelled()
  {
    return cancelled;
  }

  public boolean isLocked()
  {
    return isLocked;
  }
}
