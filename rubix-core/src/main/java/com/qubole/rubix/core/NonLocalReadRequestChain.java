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

import com.google.common.base.Throwables;
import com.qubole.rubix.spi.BookKeeperClient;
import com.qubole.rubix.spi.CacheConfig;
import com.qubole.rubix.spi.CachingConfigHelper;
import com.qubole.rubix.spi.DataRead;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;

import static com.qubole.rubix.spi.BookKeeperClient.createBookKeeperClient;

/**
 * Created by qubole on 31/8/16.
 */
public class NonLocalReadRequestChain extends DirectReadRequestChain
{
    private final String filePath;
    String remoteNodeName;
    BookKeeperClient bookKeeperClient;
    Configuration conf;
    private boolean strictMode = false;

    private static final Log log = LogFactory.getLog(ReadRequestChain.class);

    public NonLocalReadRequestChain(String remoteNodeName, Configuration conf, FSDataInputStream inputStream, String remotePath)
    {
        super(inputStream);
        this.remoteNodeName = remoteNodeName;
        this.conf = conf;
        this.strictMode = CachingConfigHelper.isStrictMode(conf);
        this.filePath = remotePath;
    }

    public ReadRequestChainStats getStats()
    {
        return new ReadRequestChainStats()
                .setRemoteReads(requests)
                .setNonLocalReads(requests)
                .setRequestedRead(totalRead)
                .setNonLocalDataRead(totalRead);
    }

    @Override
    public Integer call()
            throws Exception
    {
        if (readRequests.size() == 0) {
            return 0;
        }

        try {
            this.bookKeeperClient = createBookKeeperClient(remoteNodeName, conf);
        }
        catch (Exception e) {
            if (strictMode) {
                throw Throwables.propagate(e);
            }
            log.warn("Could not create BookKeeper Client " + Throwables.getStackTraceAsString(e));
            bookKeeperClient = null;
        }
        //this might interrupt remoteReadRequests
        if (bookKeeperClient == null) {
            super.call();
        }

        for (ReadRequest readRequest : readRequests) {
            int readLength = 0;
            int currentPosition = readRequest.destBufferOffset;
            int lengthRemaining = readRequest.getActualReadLength();
            DataRead dataRead = null;
            int bufferLength = CacheConfig.getBufferSize();

            while (lengthRemaining > 0) {
                if (lengthRemaining < bufferLength) {
                    bufferLength = lengthRemaining;
                }
                dataRead = bookKeeperClient.readData(filePath, currentPosition, bufferLength);
                readLength += dataRead.sizeRead;
                lengthRemaining = readRequest.getActualReadLength() - readLength;
                totalRead += dataRead.sizeRead;
                currentPosition += bufferLength;
            }

            readRequest.destBuffer = dataRead.getData();
        }

        return totalRead;
    }
}
