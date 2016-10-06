package com.qubole.rubix.spi;

import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransport;

import java.util.List;

/**
 * Created by qubole on 6/10/16.
 */
public class LocalBookKeeperClient
        extends BookKeeperClient
{
    BookKeeperService.Iface bookKeeper = null;
    public LocalBookKeeperClient(TTransport transport, BookKeeperService.Iface bookKeeper)
    {
        super(transport);
        this.bookKeeper = bookKeeper;
    }

    public static BookKeeperClient createBookKeeperClient(Configuration conf, BookKeeperService.Iface bookKeeper)
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
}
