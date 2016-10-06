package com.qubole.rubix.spi;

import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.transport.TTransportException;

/**
 * Created by qubole on 5/10/16.
 */
public class BookKeeperFactory
{
    BookKeeperService.Iface bookKeeper = null;

    public BookKeeperFactory() {
    }

    public BookKeeperFactory(BookKeeperService.Iface bookKeeper) {
        this.bookKeeper = bookKeeper;
    }
    public BookKeeperClient createBookKeeperClient(Configuration conf)
            throws TTransportException
    {
        if (bookKeeper == null) {
            return BookKeeperClient.createBookKeeperClient(conf);
        }
        else {
            return LocalBookKeeperClient.createBookKeeperClient(conf, bookKeeper);
        }
    }
}
