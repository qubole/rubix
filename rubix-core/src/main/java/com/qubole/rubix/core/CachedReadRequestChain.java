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

import java.io.IOException;
import java.io.RandomAccessFile;

import static com.google.common.base.Preconditions.checkState;

/**
 * Created by stagra on 4/1/16.
 */
public class CachedReadRequestChain extends ReadRequestChain
{
    private final RandomAccessFile fileToRead;
    private int read = 0; // data read

    private static final Log log = LogFactory.getLog(CachedReadRequestChain.class);

    public CachedReadRequestChain(RandomAccessFile fileToRead)
    {
        this.fileToRead = fileToRead;
    }

    public Integer call()
            throws IOException
    {
        // TODO: any exception here should not cause workload to fail
        // rather should be retried and eventually read from backend
        Thread.currentThread().setName(threadName);

        if (readRequests.size() == 0) {
            return 0;
        }

        checkState(isLocked, "Trying to execute Chain without locking");
        for (ReadRequest readRequest : readRequests) {
            fileToRead.seek(readRequest.getActualReadStart());
            int nread = 0;
            while (nread < readRequest.getActualReadLength()) {
                int nbytes = fileToRead.read(readRequest.getDestBuffer(), readRequest.getDestBufferOffset() + nread, readRequest.getActualReadLength() - nread);
                log.debug(String.format("CachedFileRead copied data from %d of length %d at buffer offset %d",
                        readRequest.getActualReadStart(),
                        readRequest.getActualReadLength() - nread,
                        readRequest.getDestBufferOffset() + nread));
                if (nbytes < 0) {
                    break;
                }
                nread += nbytes;
            }
            read += nread;
        }
        //log.info(String.format("Read %d bytes from cached file", read));
        return read;
    }

    public ReadRequestChainStats getStats()
    {
        return new ReadRequestChainStats()
                .setCachedDataRead(read)
                .setCachedReads(requests);
    }
}
