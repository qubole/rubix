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

import com.qubole.rubix.spi.DataTransferClientHelper;
import com.qubole.rubix.spi.DataTransferHeader;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import static com.google.common.base.Preconditions.checkState;

/**
 * Created by sakshia on 31/8/16.
 */
public class NonLocalReadRequestChain extends ReadRequestChain
{
    long fileSize;
    String filePath;
    long lastModified;
    String remoteNodeName;
    Configuration conf;
    int totalRead = 0;
    int directRead = 0;
    FileSystem remoteFileSystem;
    FSDataInputStream inputStream = null;
    int clusterType;

    private static final Log log = LogFactory.getLog(NonLocalReadRequestChain.class);

    public NonLocalReadRequestChain(String remoteLocation, long fileSize, long lastModified, Configuration conf, FileSystem remoteFileSystem, String remotePath, int clusterType)
    {
        this.remoteNodeName = remoteLocation;
        this.remoteFileSystem = remoteFileSystem;
        this.lastModified = lastModified;
        this.filePath = remotePath;
        this.fileSize = fileSize;
        this.conf = conf;
        this.clusterType = clusterType;
    }

    public ReadRequestChainStats getStats()
    {
        return new ReadRequestChainStats()
                .setNonLocalReads(requests)
                .setRequestedRead(totalRead + directRead)
                .setNonLocalDataRead(totalRead);
    }

    @Override
    public Integer call()
            throws Exception
    {
        Thread.currentThread().setName(threadName);
        if (readRequests.size() == 0) {
            return 0;
        }
        checkState(isLocked, "Trying to execute Chain without locking");

        for (ReadRequest readRequest : readRequests) {
            SocketChannel dataTransferClient;
            try {
                dataTransferClient = DataTransferClientHelper.createDataTransferClient(remoteNodeName, conf);
            }
            catch (Exception e) {
                log.info("Could not create Data Transfer Client ", e);
                return directReadRequest(readRequests.indexOf(readRequest));
            }
            try {
                int nread = 0;
                ByteBuffer buf = DataTransferClientHelper.writeHeaders(conf, new DataTransferHeader(readRequest.getActualReadStart(),
                        readRequest.getActualReadLength(), fileSize, lastModified, clusterType, filePath));

                dataTransferClient.write(buf);
                int bytesread = 0;
                ByteBuffer dst = ByteBuffer.wrap(readRequest.destBuffer, readRequest.getDestBufferOffset(), readRequest.destBuffer.length - readRequest.getDestBufferOffset());
                while (bytesread != readRequest.getActualReadLength()) {
                    nread = dataTransferClient.read(dst);
                    bytesread += nread;
                    totalRead += nread;
                    if (nread == -1) {
                        totalRead -= bytesread;
                        throw new Exception("Error in Local Transfer Server");
                    }
                    dst.position(bytesread + readRequest.getDestBufferOffset());
               }
            }
            catch (Exception e) {
                log.info("Error reading data from node : " + remoteNodeName, e);
                return directReadRequest(readRequests.indexOf(readRequest));
            }
            finally {
                try {
                    log.info(String.format("Read %d bytes internally from node %s", totalRead, remoteNodeName));
                    dataTransferClient.close();
                }
                catch (IOException e) {
                    log.info("Error closing Data Transfer client : " + remoteNodeName, e);
                }
            }
        }

        return totalRead;
    }

    private int directReadRequest(int index)
            throws Exception
    {
        inputStream = remoteFileSystem.open(new Path(filePath));
        DirectReadRequestChain readChain = new DirectReadRequestChain(inputStream);
        for (ReadRequest readRequest : readRequests.subList(index, readRequests.size())) {
            readChain.addReadRequest(readRequest);
        }
        readChain.lock();
        directRead = readChain.call();
        return (totalRead + directRead);
    }
}
