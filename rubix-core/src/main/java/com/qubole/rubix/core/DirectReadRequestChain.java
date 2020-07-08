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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;

import java.io.IOException;

import static com.google.common.base.Preconditions.checkState;
import static com.qubole.rubix.spi.CacheUtil.UNKONWN_GENERATION_NUMBER;
import static java.lang.String.format;

/**
 * Created by stagra on 17/2/16.
 * <p>
 * This chain read directly from Remote. This is like reading from ParentFS directly
 */
public class DirectReadRequestChain extends ReadRequestChain
{
  private final FSDataInputStream inputStream;
  private long totalRead;

  private static final Log log = LogFactory.getLog(DirectReadRequestChain.class);

  public DirectReadRequestChain(FSDataInputStream inputStream)
  {
    super(UNKONWN_GENERATION_NUMBER);
    this.inputStream = inputStream;
  }

  @Override
  public ReadRequestChainStats getStats()
  {
    return new ReadRequestChainStats()
        .setRemoteReads(requests)
        .setRequestedRead(totalRead);
  }

  @Override
  public Long call() throws IOException
  {
    log.debug(String.format("Read Request threadName: %s, Direct read Executor threadName: %s", threadName, Thread.currentThread().getName()));
    Thread.currentThread().setName(threadName);
    long startTime = System.currentTimeMillis();

    if (readRequests.size() == 0) {
      return 0L;
    }

    checkState(isLocked, "Trying to execute Chain without locking");

    for (ReadRequest readRequest : readRequests) {
      if (cancelled) {
        propagateCancel(this.getClass().getName());
      }
      try {
        inputStream.readFully(readRequest.actualReadStart, readRequest.getDestBuffer(), readRequest.getDestBufferOffset(), readRequest.getActualReadLengthIntUnsafe());
      }
      catch (Exception e) {
        log.error(format("Error reading %d bytes directly from remote at position %d", readRequest.getActualReadLengthIntUnsafe(), readRequest.actualReadStart), e);
        throw e;
      }
      totalRead += readRequest.getActualReadLengthIntUnsafe();
    }
    log.debug(String.format("Read %d bytes directly from remote, no caching", totalRead));
    log.debug("DirectReadRequest took : " + (System.currentTimeMillis() - startTime) + " msecs ");
    return totalRead;
  }
}
