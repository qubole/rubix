package com.qubole.rubix.core;

import com.qubole.rubix.bookkeeper.BookKeeper;
import com.qubole.rubix.bookkeeper.BookKeeperClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.transport.TTransportException;

/**
 * Created by qubole on 5/10/16.
 */
public class BookKeeperFactory
{
    public BookKeeperClient createBookKeeperClient(Configuration conf, BookKeeper bookKeeper)
            throws TTransportException
    {
        if (bookKeeper == null) {
            return BookKeeperClient.createBookKeeperClient(conf);
        }
        else {
            return BookKeeperClient2.createBookKeeperClient(bookKeeper, conf);
        }
    }
}
