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
 */
public abstract class ReadRequestChain implements Callable<Integer>
{
  List<ReadRequest> readRequests = new ArrayList<ReadRequest>();
  ReadRequest lastRequest;
  boolean isLocked;
  boolean cancelled;

  protected String threadName;
  protected long requests;

  private static final Log log = LogFactory.getLog(ReadRequestChain.class);

  public ReadRequestChain()
  {
    super();
    this.threadName = Thread.currentThread().getName();
  }

  // Should be added in forward seek fashion for better performance
  public void addReadRequest(ReadRequest readRequest)
  {
    checkState(!isLocked, "Adding request to a locked chain");
    log.debug(String.format("Request to add ReadRequest: [%d, %d, %d, %d, %d]", readRequest.getBackendReadStart(), readRequest.getBackendReadEnd(), readRequest.getActualReadStart(), readRequest.getActualReadEnd(), readRequest.getDestBufferOffset()));
    if (lastRequest == null) {
      addRequest(readRequest);
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
        addRequest(readRequest);
      }
    }
    requests++;
  }

  private void addRequest(ReadRequest readRequest)
  {
    readRequests.add(readRequest);
    lastRequest = readRequest;
    log.debug(String.format("Added ReadRequest: [%d, %d, %d, %d, %d]", readRequest.getBackendReadStart(), readRequest.getBackendReadEnd(), readRequest.getActualReadStart(), readRequest.getActualReadEnd(), readRequest.getDestBufferOffset()));
  }

  public void lock()
  {
    isLocked = true;
  }

  @VisibleForTesting
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

  private class CancelledException
      extends IOException
  {
    public CancelledException(String message)
    {
      super(message);
    }
  }
}
