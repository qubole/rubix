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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;

import static com.google.common.base.Preconditions.checkState;

/**
 * Created by stagra on 17/2/16.
 * <p>
 * This chain read directly from Remote. This is like reading from ParentFS directly
 */
public class DirectReadRequestChain extends ReadRequestChain
{
  FSDataInputStream inputStream;
  int totalRead;

  private static final Log log = LogFactory.getLog(DirectReadRequestChain.class);

  public DirectReadRequestChain(FSDataInputStream inputStream)
  {
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
  public Integer call()
      throws Exception
  {
    Thread.currentThread().setName(threadName);
    long startTime = System.currentTimeMillis();

    if (readRequests.size() == 0) {
      return 0;
    }

    checkState(isLocked, "Trying to execute Chain without locking");

    for (ReadRequest readRequest : readRequests) {
      if (cancelled) {
        propagateCancel(this.getClass().getName());
      }
      inputStream.seek(readRequest.actualReadStart);
      int nread = 0;
      while (nread < readRequest.getActualReadLength()) {
        int nbytes = inputStream.read(readRequest.getDestBuffer(), readRequest.getDestBufferOffset() + nread, readRequest.getActualReadLength() - nread);
        if (nbytes < 0) {
          log.info(String.format("Returning Read %d bytes directly from remote, no caching", totalRead));
          return nread;
        }
        nread += nbytes;
      }
      totalRead += nread;
    }
    log.info(String.format("Read %d bytes directly from remote, no caching", totalRead));
    log.debug("DirectReadRequest took : " + (System.currentTimeMillis() - startTime) + " msecs ");
    return totalRead;
  }
}
