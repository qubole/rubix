package com.qubole.rubix.spi;

import com.qubole.rubix.bookkeeper.BookKeeper;
import com.qubole.rubix.spi.BookKeeperClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.transport.TTransportException;

/**
 * Created by qubole on 5/10/16.
 */
public class BookKeeperFactory
{
    public BookKeeperFactory()
    public BookKeeperClient createBookKeeperClient(Configuration conf, com.qubole.rubix.spi.BookKeeperService.Iface bookKeeper)
            throws TTransportException
    {
        if (bookKeeper == null) {
            return BookKeeperClient.createBookKeeperClient(conf);
        }
        else {
            return LocalBookKeeperClient.createBookKeeperClient(bookKeeper, conf);
        }
    }
}
