package com.qubole.rubix.core;

import com.qubole.rubix.bookkeeper.BookKeeper;
import com.qubole.rubix.bookkeeper.BookKeeperClient;
import com.qubole.rubix.spi.BlockLocation;
import com.qubole.rubix.spi.BookKeeperService;
import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransport;

import java.util.List;

/**
 * Created by qubole on 6/10/16.
 */
public class BookKeeperClient2
        extends BookKeeperClient
{
    public static BookKeeper bookKeeper = null;

    public BookKeeperClient2(TTransport transport)
    {
        super(transport);
    }

 /*   public BookKeeperClient2(BookKeeper bookKeeper, Configuration conf)
    {
        this.bookKeeper = bookKeeper;
    }
*/
    public static BookKeeperClient createBookKeeperClient(BookKeeper bookKeeper, Configuration conf)
    {
        TTransport transport= null;
        BookKeeperClient2.bookKeeper = bookKeeper;
        return new BookKeeperClient2(transport);
    }

    @Override
    public List<BlockLocation> getCacheStatus(String remotePath, long fileLength, long lastModified, long startBlock, long endBlock, int clusterType)
    {


    }
}
