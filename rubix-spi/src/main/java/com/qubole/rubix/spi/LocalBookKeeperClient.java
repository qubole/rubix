package com.qubole.rubix.spi;

import com.qubole.rubix.bookkeeper.BookKeeper;
import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.transport.TTransport;

import java.util.List;

/**
 * Created by qubole on 6/10/16.
 */
public class LocalBookKeeperClient
        extends BookKeeperClient
{
    public static BookKeeper bookKeeper = null;

    public LocalBookKeeperClient(TTransport transport, BookKeeper bookKeeper)
    {
        super(transport);
    }

    public static BookKeeperClient createBookKeeperClient(BookKeeper bookKeeper, Configuration conf)
    {
        TTransport transport= null;
        LocalBookKeeperClient.bookKeeper = bookKeeper;
        return new LocalBookKeeperClient(transport);
    }

    @Override
    public List<BlockLocation> getCacheStatus(String remotePath, long fileLength, long lastModified, long startBlock, long endBlock, int clusterType)
    {


    }
}
