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
import com.qubole.rubix.spi.BookKeeperFactory;
import com.qubole.rubix.spi.CacheConfig;
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
        SocketChannel transferClient;
        try {
            BookKeeperFactory bookKeeperFactory = new BookKeeperFactory();
            transferClient = bookKeeperFactory.createTransferClient(remoteNodeName, conf);
        }
        catch (Exception e) {
            log.info("Could not create Transfer Client " + Throwables.getStackTraceAsString(e));
            return directReadRequest(0);
        }

        for (ReadRequest readRequest : readRequests) {
            int nread = 0;
            ByteBuffer buf = ByteBuffer.allocate(CacheConfig.getDataTransferBufferSize(conf));
            /* order is: long : offset, int : readLength, long : fileSize, long : lastModified,
                int : clusterType, int : filePathLength, String : filePath */
            buf.putLong(readRequest.getActualReadStart());
            buf.putInt(readRequest.getActualReadLength());
            buf.putLong(fileSize);
            buf.putLong(lastModified);
            buf.putInt(clusterType);
            buf.putInt(filePath.length());
            buf.put(filePath.getBytes());
            buf.flip();
            if (transferClient.isConnected()) {
                try {
                    transferClient.write(buf);
                }
                catch (IOException e) {
                    e.printStackTrace();
                    transferClient.close();
                    return directReadRequest(readRequests.indexOf(readRequest));
                }
            }
            int bytesread = 0;
            ByteBuffer dst = ByteBuffer.wrap(readRequest.destBuffer, readRequest.getDestBufferOffset(), readRequest.destBuffer.length - readRequest.getDestBufferOffset());
            while (bytesread != readRequest.getActualReadLength()) {
                if (transferClient.isConnected()) {
                    try {
                        nread = transferClient.read(dst);
                        bytesread += nread;
                        totalRead += nread;
                    }
                    catch (IOException e) {
                        e.printStackTrace();
                        return directReadRequest(readRequests.indexOf(readRequest));
                    }
                }
                if (nread == -1) {
                    totalRead += 1;
                    log.info("Error in Local Transfer Server");
                    return directReadRequest(readRequests.indexOf(readRequest));
                }
                dst.position(bytesread + readRequest.getDestBufferOffset());
            }

            transferClient.close();
        }

        log.info(String.format("Read %d bytes internally from node %s", totalRead, remoteNodeName));
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
