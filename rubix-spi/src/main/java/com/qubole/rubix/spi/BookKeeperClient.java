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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import java.io.Closeable;
import java.io.IOException;

/**
 * Created by stagra on 16/2/16.
 */
public class BookKeeperClient extends BookKeeperService.Client
        implements Closeable
{
    private static Log log = LogFactory.getLog(BookKeeperClient.class.getName());
    TTransport transport;

    public BookKeeperClient(TTransport transport)
    {
        super(new TBinaryProtocol(transport));
        this.transport = transport;
    }
    public BookKeeperClient(TProtocol prot)
    {
        super(prot);
        this.transport = prot.getTransport();
    }

    @Override
    public void close()
            throws IOException
    {
        transport.close();
    }

    // Caller responsible to close the client
    public static BookKeeperClient createBookKeeperClient(Configuration conf)
            throws TTransportException
    {
        TTransport transport;
        transport = new TSocket("localhost", CacheConfig.getServerPort(conf));
        transport.open();

        BookKeeperClient client = new BookKeeperClient(transport);
        return client;
    }

    public static BookKeeperClient createBookKeeperClient(String hostName, Configuration conf)
            throws TTransportException
    {
        TTransport transport;
        transport = new TSocket(hostName, CacheConfig.getServerPort(conf));
        transport.open();

        BookKeeperClient client = new BookKeeperClient(transport);
        return client;
    }
}
