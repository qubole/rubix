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
package com.qubole.rubix.bookkeeper;

/**
 * Created by sakshia on 27/9/16.
 */

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import com.google.common.base.Throwables;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class RetryingBookkeeperClient
        extends BookKeeperService.Client
        implements Closeable
{
    private static final Logger LOG = LoggerFactory.getLogger(RetryingBookkeeperClient.class);
    private int maxRetries;
    private BookKeeperClient bookKeeperClient = null;

    /**
     * List of causes for TTransportException
     * ALREADY_OPEN
     * END_OF_FILE
     * NOT_OPEN
     * TIMED_OUT
     * type_
     * UNKNOWN
     */

    public RetryingBookkeeperClient(BookKeeperClient client, int maxRetries)
    {
        super(new TBinaryProtocol(client.transport));
        this.bookKeeperClient = client;
        this.maxRetries = maxRetries;
    }

    @Override
    public List<Location> getCacheStatus(String remotePath, long fileLength, long lastModified, long startBlock, long endBlock, int clusterType)
    {
        try {
            return super.getCacheStatus(remotePath, fileLength, lastModified, startBlock, endBlock, clusterType);
        }
        catch (TException e) {
            try {
                retryConnection();
                return super.getCacheStatus(remotePath, fileLength, lastModified, startBlock, endBlock, clusterType);
            }
            catch (TException e1) {
                LOG.info("Could not get cache status from server " + Throwables.getStackTraceAsString(e));
            }
        }
        return null;
    }

    @Override
    public void setAllCached(String remotePath, long fileLength, long lastModified, long startBlock, long endBlock)
    {
        try {
            super.setAllCached(remotePath, fileLength, lastModified, startBlock, endBlock);
        }
        catch (TException e) {
            try {
                retryConnection();
                super.setAllCached(remotePath, fileLength, lastModified, startBlock, endBlock);
            }
            catch (TException e1) {
                LOG.info("\"Could not update BookKeeper about newly cached blocks: " + Throwables.getStackTraceAsString(e));
            }
        }
    }

    private void retryConnection()
            throws TTransportException
    {
        int errors = 0;
        bookKeeperClient.transport.close();

        while (errors < maxRetries) {
            try {
                bookKeeperClient.transport.open();
            }
            catch (TTransportException e1) {
                LOG.error("Error while reconnecting:", e1);
                bookKeeperClient.transport.close();
                errors++;
            }
        }

        if (errors >= maxRetries) {
            throw new TTransportException("Failed to reconnect");
        }
    }

    @Override
    public void close()
            throws IOException
    {
        bookKeeperClient.close();
    }
}
