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
package com.qubole.rubix.spi;

import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransport;

import java.io.IOException;
import java.util.List;

/**
 * Created by sakshia on 6/10/16.
 */
public class LocalBookKeeperClient
        extends RetryingBookkeeperClient
{
    BookKeeperService.Iface bookKeeper = null;
    public LocalBookKeeperClient(TTransport transport, BookKeeperService.Iface bookKeeper)
    {
        super(transport, 1);
        this.bookKeeper = bookKeeper;
    }

    public static RetryingBookkeeperClient createBookKeeperClient(Configuration conf, BookKeeperService.Iface bookKeeper)
    {
        TTransport transport = null;
        return new LocalBookKeeperClient(transport, bookKeeper);
    }

    @Override
    public List<BlockLocation> getCacheStatus(String remotePath, long fileLength, long lastModified, long startBlock, long endBlock, int clusterType)
            throws TException
    {
        return bookKeeper.getCacheStatus(remotePath, fileLength, lastModified, startBlock, endBlock, clusterType);
    }

    @Override
    public void close()
            throws IOException
    {
    }
}
