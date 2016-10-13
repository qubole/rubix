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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.thrift.shaded.TException;

import static com.qubole.rubix.spi.BookKeeperClient.createBookKeeperClient;

/**
 * Created by sakshia on 31/8/16.
 */
public class NonLocalReadRequestChain extends ReadRequestChain
{
    private final String filePath;
    String remoteNodeName;
    BookKeeperClient bookKeeperClient;
    Configuration conf;
    private boolean strictMode = false;
    int sizeRead = 0;
    FileSystem fs;
    FSDataInputStream inputStream = null;

    private static final Log log = LogFactory.getLog(ReadRequestChain.class);

    public NonLocalReadRequestChain(String remoteNodeName, Configuration conf, FileSystem fs, String remotePath)
    {
        this.remoteNodeName = remoteNodeName;
        this.fs = fs;
        this.conf = conf;
        this.strictMode = CachingConfigHelper.isStrictMode(conf);
        this.filePath = remotePath;
    }

    public ReadRequestChainStats getStats()
    {
        return new ReadRequestChainStats()
                .setRemoteReads(requests)
                .setNonLocalReads(requests)
                .setRequestedRead(sizeRead)
                .setNonLocalDataRead(sizeRead);
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
            return directReadRequest();
        }

        for (ReadRequest readRequest : readRequests) {
            int readLength = 0;
            int offset = readRequest.getDestBufferOffset();
            long readStart = readRequest.getActualReadStart();
            int lengthRemaining = readRequest.getActualReadLength();
            DataRead dataRead;
            int bufferLength = CacheConfig.getBufferSize();

            while (lengthRemaining > 0) {
                if (lengthRemaining < bufferLength) {
                    bufferLength = lengthRemaining;
                }
                try {
                    dataRead = bookKeeperClient.readData(filePath, readStart + readLength, offset, bufferLength);
                }
                catch (TException e) {
                    return directReadRequest();
                }
                System.arraycopy(dataRead.getData(), 0, readRequest.destBuffer, readRequest.getDestBufferOffset() + readLength, dataRead.getSizeRead());
                readLength += dataRead.getSizeRead();
                lengthRemaining = readRequest.getActualReadLength() - readLength;
                sizeRead += dataRead.getSizeRead();
            }
        }

        if (bookKeeperClient != null) {
            bookKeeperClient.close();
        }
        log.info(String.format("Read %d bytes directly from node %s", sizeRead, remoteNodeName));
        return sizeRead;
    }

    private int directReadRequest()
            throws Exception
    {
        int read;
        inputStream = fs.open(new Path(filePath));
        DirectReadRequestChain readChain = new DirectReadRequestChain(inputStream);
        for (ReadRequest readRequest : readRequests) {
            readChain.addReadRequest(readRequest);
        }
        readChain.lock();
        read = readChain.call();
        return read;
    }
}
