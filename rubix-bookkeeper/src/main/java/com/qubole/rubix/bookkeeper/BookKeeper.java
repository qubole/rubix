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

import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalCause;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.cache.Weigher;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.qubole.rubix.spi.CachingConfigHelper;
import com.qubole.rubix.spi.ClusterManager;
import com.qubole.rubix.spi.CacheConfig;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.TException;
import com.qubole.rubix.hadoop2.hadoop2CM.Hadoop2ClusterManager;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Created by stagra on 12/2/16.
 */
public class BookKeeper
        implements com.qubole.rubix.bookkeeper.BookKeeperService.Iface
{
    private static Cache<String, FileMetadata> fileMetadataCache;
    private static ClusterManager clusterManager = null;
    private static Log log = LogFactory.getLog(BookKeeper.class.getName());
    private long totalRequests = 0;
    private long cachedRequests = 0;
    private Set<String> localSplits;
    private Configuration conf;
    List<String> nodes;
    public enum Cluster {Hadoop2ClusterManager, PrestoClusterManager, HadoopClusterManager}
    public enum State {Cached, Local, Non_Local}
    public BookKeeper(Configuration conf)
    {
        this.conf = conf;
        initializeCache(conf);
    }

    @Override
    public List<Integer> getCacheStatus(String remotePath, long fileLength, long lastModified, long startBlock, long endBlock, int c)
            throws TException
    {
        if (clusterManager == null) {
            if (c == 0) {
                clusterManager = new Hadoop2ClusterManager();
                clusterManager.initialize(conf);
                nodes = clusterManager.getNodes();
                log.info("Nodes are : " + nodes);
            }
        }
        String nodename = null;
        try {
            nodename = InetAddress.getLocalHost().getCanonicalHostName();
        }
        catch (UnknownHostException e) {
            e.printStackTrace();
        }

        localSplits = CachingConfigHelper.getLocalityInfo(conf, nodename, remotePath);

        if (localSplits.isEmpty()) {
            Map<String, StringBuilder> nodeSplits = new HashMap<String, StringBuilder>();
            int blockNumber = 0;
            for (long i = 0; i < fileLength; i = i + clusterManager.getSplitSize()) {
                long end = i + clusterManager.getSplitSize();
                if (end > fileLength) {
                    end = fileLength;
                }
                String key = remotePath + i + end;
                HashFunction hf = Hashing.md5();
                HashCode hc = hf.hashString(key, Charsets.UTF_8);
                int nodeIndex = Hashing.consistentHash(hc, nodes.size());
                String[] name = new String[] {nodes.get(nodeIndex)};
                StringBuilder blocks = nodeSplits.get(name[0]);
                if (blocks == null) {
                    blocks = new StringBuilder();
                    nodeSplits.put(name[0], blocks);
                }
                else {
                    blocks.append(",");
                }
                blocks.append(blockNumber);
                blockNumber++;
            }
                for (Map.Entry<String, StringBuilder> nodeSplit : nodeSplits.entrySet()) {
                    log.info("node is: " + nodeSplit.getKey() + "Split is: " + nodeSplit.getValue().toString());
                    CachingConfigHelper.setLocalityInfo(conf, nodeSplit.getKey(), remotePath, nodeSplit.getValue().toString());
                }

            localSplits = CachingConfigHelper.getLocalityInfo(conf, nodename, remotePath);
        }
        log.info("LocalSplits are : " + localSplits + " for nodename " + nodename);
        FileMetadata md;
        try {
            md = fileMetadataCache.get(remotePath, new CreateFileMetadataCallable(remotePath, fileLength, lastModified, conf));
            if (md.getLastModified() != lastModified) {
                invalidate(remotePath);
                md = fileMetadataCache.get(remotePath, new CreateFileMetadataCallable(remotePath, fileLength, lastModified, conf));
            }
        }
        catch (ExecutionException e) {
            log.error(String.format("Could not fetch Metadata for %s : %s", remotePath, Throwables.getStackTraceAsString(e)));
            throw new TException(e);
        }
        endBlock = setCorrectEndBlock(endBlock, fileLength, remotePath);
        List<Integer> blocksInfo = new ArrayList<Integer>((int) (endBlock - startBlock));

        //log.info("RemotePath is" + remotePath + "and LocalSplits");
        int blockSize = CacheConfig.getBlockSize(conf);
        for (long blockNum = startBlock; blockNum < endBlock; blockNum++) {
            totalRequests++;
            long split = (blockNum * blockSize) /  clusterManager.getSplitSize();

            log.info("split to check is " + split + " and blocknum is " + blockNum);
            if (md.isBlockCached(blockNum)) {
                blocksInfo.add(State.Cached.ordinal());
                cachedRequests++;
            }
            else {
                if (localSplits.contains(Long.toString(split))) {
                    blocksInfo.add(State.Local.ordinal());
                }
                else {
                    blocksInfo.add(State.Non_Local.ordinal());
                }
            }
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
        if (md == null) {
            return;
        }
        if (md.getLastModified() != lastModified) {
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

    @Override
    public Map getCacheStats()
    {
        Map<String, Double> stats = new HashMap<String, Double>();
        stats.put("Cache Hit Rate", ((double) cachedRequests / totalRequests));
        stats.put("Cache Miss Rate", ((double) (totalRequests - cachedRequests) / totalRequests));
        stats.put("Cache Reads", ((double) cachedRequests));
        stats.put("Remote Reads", ((double) (totalRequests  - cachedRequests)));
        return stats;
    }

    private long setCorrectEndBlock(long endBlock, long fileLength, String remotePath)
    {
        long lastBlock = (fileLength - 1) / CacheConfig.getBlockSize(conf);

        if (endBlock > (lastBlock + 1)) {
            log.debug(String.format("Correct endBlock from %d to %d for path %s and length %d", endBlock, lastBlock + 1, remotePath, fileLength));
            endBlock = lastBlock + 1;
        }

        return endBlock;
    }

    private static synchronized void initializeCache(final Configuration conf)
    {
        long avail = 0;
        for (int d = 0; d < CacheConfig.numDisks(conf); d++) {
            avail += new File(CacheConfig.getDirPath(conf, d)).getUsableSpace();
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
                .maximumWeight((long) (avail * 1.0 * CacheConfig.getCacheDataFullnessPercentage(conf) / 100.0))
                .expireAfterWrite(CacheConfig.getCacheDataExpirationAfterWrite(conf), TimeUnit.SECONDS)
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
                                for (int d = 0; d < CacheConfig.numDisks(conf); d++) {
                                    free += new File(CacheConfig.getDirPath(conf, d)).getUsableSpace();
                                }
                                if (free > total * 1.0 * (100.0 - CacheConfig.getCacheDataFullnessPercentage(conf) / 100)) {
                                    // still havent utilized the allowed space so do not delete the backing file
                                    md.close();
                                    log.warn("Evicting " + md.getRemotePath().toString() + " due to " + notification.getCause());
                                    return;
                                }
                            }
                            //if file has been modified in cloud, its entry will be deleted due to "EXPLICIT"
                            log.warn("deleting entry for" + md.getRemotePath().toString() + " due to "
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

    private static class CreateFileMetadataCallable
            implements Callable<FileMetadata>
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
            this.lastModified = lastModified;
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
