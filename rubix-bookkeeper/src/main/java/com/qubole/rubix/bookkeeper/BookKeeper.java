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

import com.google.common.base.Throwables;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalCause;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.cache.Weigher;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.TException;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Created by stagra on 12/2/16.
 */
public class BookKeeper implements BookKeeperService.Iface
{
    private static Cache<String, FileMetadata> fileMetadataCache;
    private static Log log = LogFactory.getLog(BookKeeper.class.getName());

    private Configuration conf;

    public BookKeeper(Configuration conf)
    {
        this.conf = conf;
        initializeCache(conf);
    }

    @Override
    public List<Boolean> getCacheStatus(String remotePath, long fileLength, long lastModified, long startBlock, long endBlock)
            throws TException
    {
        FileMetadata md;
        try {
            md = fileMetadataCache.get(remotePath, new CreateFileMetadataCallable(remotePath, fileLength, lastModified, conf));
            if( md.getLastModified() != lastModified ) {
                invalidate(remotePath);
                md = fileMetadataCache.get(remotePath, new CreateFileMetadataCallable(remotePath, fileLength, lastModified, conf));
            }
        }
        catch (ExecutionException e) {
            log.error(String.format("Could not fetch Metadata for %s : %s", remotePath, Throwables.getStackTraceAsString(e)));
            throw new TException(e);
        }

        endBlock = setCorrectEndBlock(endBlock, fileLength, remotePath);

        List<Boolean> blocksInfo = new ArrayList<Boolean>((int)(endBlock - startBlock));
        for (long blockNum = startBlock; blockNum < endBlock; blockNum++) {
            blocksInfo.add(md.isBlockCached(blockNum));
        }


        return blocksInfo;
    }

    @Override
    public void setAllCached(String remotePath, long fileLength, long lastModified, long startBlock, long endBlock)
            throws TException
    {
        FileMetadata md;
        md = fileMetadataCache.getIfPresent(remotePath);

        //md will be null when 2 users try to update the file in parallel and both their entries are invalidated.
        // TODO: find a way to optimize this so that the file doesn't have to be read again in next request (new data is stored instead of invalidation)
        if( md == null || md.getLastModified() != lastModified ) {
            invalidate(remotePath);
            return;
        }
        endBlock = setCorrectEndBlock(endBlock, fileLength, remotePath);

        synchronized (md) {
            for (long blockNum = startBlock; blockNum < endBlock; blockNum++) {
                md.setBlockCached(blockNum);
            }
        }
    }

    private long setCorrectEndBlock(long endBlock, long fileLength, String remotePath)
    {
        long lastBlock = (fileLength - 1) / BookKeeperConfig.getBlockSize(conf);

        if (endBlock > (lastBlock + 1)) {
            log.debug(String.format("Correct endBlock from %d to %d for path %s and length %d", endBlock, lastBlock + 1, remotePath, fileLength));
            endBlock = lastBlock + 1;
        }

        return endBlock;
    }

    private static synchronized void initializeCache(final Configuration conf)
    {
        long avail = 0;
        for (int d = 0; d < BookKeeperConfig.numDisks(conf); d++) {
            avail += new File(BookKeeperConfig.getDirPath(conf, d)).getUsableSpace();
        }
        avail = avail / 1024 / 1024;
        final long total = avail;
        log.info("total free space " + avail + "MB");
        fileMetadataCache = CacheBuilder.newBuilder()
                .weigher(new Weigher<String, FileMetadata>()
                {
                    @Override
                    public int weigh(String key, FileMetadata md)
                    {
                        // weights are in MB to avoid overflowing due to large files
                        // This is not accurate, we are placing weight as whole filesize
                        // Rather it should be dynamic and should be equal to size of file data cached
                        // But guava needs weight fixed at init
                        // TODO: find a way to set weight accurately and get away from current workaround
                        int weight = (int) (md.getOccupiedSize() / 1024 / 1024);
                        log.info("weighing key " + key + " as " + weight);
                        return weight;
                    }
                })
                .maximumWeight((long) (avail * 1.0 * BookKeeperConfig.getCacheDataFullnessPercentage(conf) / 100.0))
                .expireAfterWrite(BookKeeperConfig.getCacheDataExpirationAfterWrite(conf), TimeUnit.SECONDS)
                .removalListener(new RemovalListener<String, FileMetadata>()
                {
                    public void onRemoval(
                            RemovalNotification<String, FileMetadata> notification)
                    {
                        try {
                            FileMetadata md = notification.getValue();
                            if (notification.getCause() == RemovalCause.EXPIRED) {
                                // This is to workaround the static weighing of Guava Cache, logic goes like this:
                                // We evict aggressively but do not delete backing data unless running out of space
                                // On next get() on cache, fileMetadata.getOccupiedSize will return size occupied on disk
                                md.close();
                                log.info("Evicting " + md.getRemotePath().toString() + " due to " + notification.getCause());
                                return;
                            }

                            if (notification.getCause() == RemovalCause.SIZE) {
                                // Here also we wont delete unless very close to disk full
                                long free = 0;
                                for (int d = 0; d < BookKeeperConfig.numDisks(conf); d++) {
                                    free += new File(BookKeeperConfig.getDirPath(conf, d)).getUsableSpace();
                                }
                                if (free > total * 1.0 * (100.0 - BookKeeperConfig.getCacheDataFullnessPercentage(conf) / 100)) {
                                    // still havent utilized the allowed space so do not delete the backing file
                                    md.close();
                                    log.info("Evicting " + md.getRemotePath().toString() + " due to " + notification.getCause());
                                    return;
                                }
                            }
                            //if file has been modified in cloud, its entry will be deleted due to "EXPLICIT"
                            log.info("deleting entry for" + md.getRemotePath().toString() + " due to "
                                    + notification.getCause());
                            md.closeAndCleanup();
                        }
                        catch (IOException e) {
                            throw Throwables.propagate(e);
                        }
                    }
                })
                .build();
    }

    private static class CreateFileMetadataCallable implements Callable<FileMetadata>
    {
        String path;
        Configuration conf;
        long fileLength;
        long lastModified;


        public CreateFileMetadataCallable(String path, long fileLength, long lastModified, Configuration conf)
        {
            this.path = path;
            this.conf = conf;
            this.fileLength = fileLength;
            this.lastModified=lastModified;
        }

        public FileMetadata call()
                throws Exception
        {
            return new FileMetadata(path, fileLength, lastModified, conf);
        }
    }

    public static void invalidate(String p)
    {
        // We might come in here with cache not initialized e.g. fs.create
        if (fileMetadataCache != null) {
            fileMetadataCache.invalidate(p);
        }
    }
}
