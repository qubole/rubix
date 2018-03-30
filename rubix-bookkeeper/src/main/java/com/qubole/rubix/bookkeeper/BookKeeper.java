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
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.cache.Weigher;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.qubole.rubix.core.ReadRequest;
import com.qubole.rubix.core.RemoteReadRequestChain;
import com.qubole.rubix.hadoop2.Hadoop2ClusterManager;
import com.qubole.rubix.presto.PrestoClusterManager;
import com.qubole.rubix.spi.BlockLocation;
import com.qubole.rubix.spi.BookKeeperFactory;
import com.qubole.rubix.spi.CacheConfig;
import com.qubole.rubix.spi.ClusterManager;
import com.qubole.rubix.spi.ClusterType;
import com.qubole.rubix.spi.Location;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.thrift.shaded.TException;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.qubole.rubix.spi.ClusterType.HADOOP2_CLUSTER_MANAGER;
import static com.qubole.rubix.spi.ClusterType.PRESTO_CLUSTER_MANAGER;
import static com.qubole.rubix.spi.ClusterType.TEST_CLUSTER_MANAGER;

/**
 * Created by stagra on 12/2/16.
 */
public class BookKeeper implements com.qubole.rubix.spi.BookKeeperService.Iface
{
  private static Cache<String, FileMetadata> fileMetadataCache;
  private static ClusterManager clusterManager;
  private static Log log = LogFactory.getLog(BookKeeper.class.getName());
  private long totalRequests;
  private long cachedRequests;
  private long remoteRequests;
  String nodeName;
  static String nodeHostName;
  static String nodeHostAddress;
  private Configuration conf;
  private static Integer lock = 1;
  private List<String> nodes;
  int currentNodeIndex = -1;
  static long splitSize;
  private RemoteFetchProcessor fetchProcessor;

  public BookKeeper(Configuration conf)
  {
    this.conf = conf;
    initializeCache(conf);
    fetchProcessor = new RemoteFetchProcessor(conf);
    fetchProcessor.startAsync();
  }

  @Override
  public List<BlockLocation> getCacheStatus(String remotePath, long fileLength, long lastModified, long startBlock, long endBlock, int clusterType)
      throws TException
  {
    initializeClusterManager(clusterType);
    if (nodeName == null) {
      log.error("Node name is null for Cluster Type" + ClusterType.findByValue(clusterType));
      return null;
    }

    if (currentNodeIndex == -1 || nodes == null) {
      log.error("Initialization not done");
      return null;
    }

    Map<Long, String> blockSplits = new HashMap<>();
    long blockNumber = 0;

    for (long i = 0; i < fileLength; i = i + splitSize) {
      long end = i + splitSize;
      if (end > fileLength) {
        end = fileLength;
      }
      String key = remotePath + i + end;
      HashFunction hf = Hashing.md5();
      HashCode hc = hf.hashString(key, Charsets.UTF_8);
      int nodeIndex = Hashing.consistentHash(hc, nodes.size());
      blockSplits.put(blockNumber, nodes.get(nodeIndex));
      blockNumber++;
    }

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
    List<BlockLocation> blockLocations = new ArrayList<>((int) (endBlock - startBlock));
    int blockSize = CacheConfig.getBlockSize(conf);

    //TODO: Store Node indices too, i.e. split to nodeIndex map and compare indices here instead of strings(nodenames).

    try {
      for (long blockNum = startBlock; blockNum < endBlock; blockNum++) {
        totalRequests++;
        long split = (blockNum * blockSize) / splitSize;
        if (!blockSplits.get(split).equalsIgnoreCase(nodeName)) {
          blockLocations.add(new BlockLocation(Location.NON_LOCAL, blockSplits.get(split)));
        }
        else {
          if (md.isBlockCached(blockNum)) {
            blockLocations.add(new BlockLocation(Location.CACHED, blockSplits.get(split)));
            cachedRequests++;
          }
          else {
            blockLocations.add(new BlockLocation(Location.LOCAL, blockSplits.get(split)));
            remoteRequests++;
          }
        }
      }
    }
    catch (IOException e) {
      throw new TException(e);
    }

    return blockLocations;
  }

  private void initializeClusterManager(int clusterType)
  {
    if (this.clusterManager == null) {
      ClusterManager clusterManager = null;
      synchronized (lock) {
        if (this.clusterManager == null) {
          try {
            nodeHostName = InetAddress.getLocalHost().getCanonicalHostName();
            nodeHostAddress = InetAddress.getLocalHost().getHostAddress();
            log.info(" HostName : " + nodeHostName + " HostAddress : " + nodeHostAddress);
          }
          catch (UnknownHostException e) {
            log.warn("Could not get nodeName", e);
            return;
          }

          if (clusterType == TEST_CLUSTER_MANAGER.ordinal()) {
            nodes = new ArrayList<>();
            nodeName = nodeHostName;
            nodes.add(nodeName);
            splitSize = 64 * 1024 * 1024;
            currentNodeIndex = 0;
            return;
          }
          else {
            if (clusterType == HADOOP2_CLUSTER_MANAGER.ordinal()) {
              clusterManager = new Hadoop2ClusterManager();
            }
            else if (clusterType == PRESTO_CLUSTER_MANAGER.ordinal()) {
              clusterManager = new PrestoClusterManager();
            }

            clusterManager.initialize(conf);
            // set the global clusterManager only after it is inited
            this.clusterManager = clusterManager;
            splitSize = clusterManager.getSplitSize();
          }
        }
      }
    }

    nodes = clusterManager.getNodes();
    if (nodes == null || nodes.size() == 0) {
      log.error("Could not initialize as no cluster node is found");
    }
    else if (nodes.indexOf(nodeHostName) >= 0) {
      currentNodeIndex = nodes.indexOf(nodeHostName);
      nodeName = nodeHostName;
    }
    else if (nodes.indexOf(nodeHostAddress) >= 0) {
      currentNodeIndex = nodes.indexOf(nodeHostAddress);
      nodeName = nodeHostAddress;
    }
    else {
      log.error(String.format("Could not initialize cluster nodes=%s nodeHostName=%s nodeHostAddress=%s " +
          "currentNodeIndex=%d", nodes, nodeHostName, nodeHostAddress, currentNodeIndex));
    }
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
    log.debug("Updating cache for " + remotePath + " StarBlock : " + startBlock + " EndBlock : " + endBlock);

    try {
      md.setBlocksCached(startBlock, endBlock);
    }
    catch (IOException e) {
      throw new TException(e);
    }
  }

  @Override
  public Map getCacheStats()
  {
    Map<String, Double> stats = new HashMap<String, Double>();
    stats.put("Cache Hit Rate", ((double) cachedRequests / (cachedRequests + remoteRequests)));
    stats.put("Cache Miss Rate", ((double) (remoteRequests) / (cachedRequests + remoteRequests)));
    stats.put("Cache Reads", ((double) cachedRequests));
    stats.put("Remote Reads", ((double) remoteRequests));
    stats.put("Non-Local Reads", ((double) (totalRequests - cachedRequests - remoteRequests)));
    return stats;
  }

  //This method is to ensure that data required by another node is cached before it is read by that node
  //using localTransferServer.
  // If the data is not already cached, remoteReadRequest for that block is sent and data is cached.

  //TODO : buffer is initialized everytime readData is called and it contains garbage value which is not required.
  // We cannot make it static because the variable is used in remoteReadRequestChain to write (cache) data to files.
  // So, it has to store the correct value in each thread. getCacheStatus is called twice.
  @Override
  public boolean readData(String remotePath, long offset, int length, long fileSize, long lastModified, int clusterType)
      throws TException
  {
    if (CacheConfig.isParallelWarmupEnabled(conf)) {
      log.info("Adding to the queue Path : " + remotePath + " Offste : " + offset + " Length " + length);
      fetchProcessor.addToProcessQueue(remotePath, offset, length, fileSize, lastModified);
      return true;
    }
    else {
      return readDataInternal(remotePath, offset, length, fileSize, lastModified, clusterType);
    }
  }

  private boolean readDataInternal(String remotePath, long offset, int length, long fileSize,
                                   long lastModified, int clusterType) throws TException
  {
    int blockSize = CacheConfig.getBlockSize(conf);
    byte[] buffer = new byte[blockSize];
    ByteBuffer byteBuffer = null;
    String localPath = CacheConfig.getLocalPath(remotePath, conf);
    FileSystem fs = null;
    FSDataInputStream inputStream = null;
    Path path = new Path(remotePath);
    long startBlock = offset / blockSize;
    long endBlock = ((offset + (length - 1)) / CacheConfig.getBlockSize(conf)) + 1;
    try {
      int idx = 0;
      List<BlockLocation> blockLocations = getCacheStatus(remotePath, fileSize, lastModified, startBlock, endBlock, clusterType);

      for (long blockNum = startBlock; blockNum < endBlock; blockNum++, idx++) {
        long readStart = blockNum * blockSize;
        log.debug(" blockLocation is: " + blockLocations.get(idx).getLocation() + " for path " + remotePath + " offset " + offset + " length " + length);
        if (blockLocations.get(idx).getLocation() != Location.CACHED) {
          if (byteBuffer == null) {
            byteBuffer = ByteBuffer.allocateDirect(CacheConfig.getDiskReadBufferSizeDefault(conf));
          }

          if (fs == null) {
            fs = path.getFileSystem(conf);
            log.info("Initializing FileSystem " + fs.toString() + " for Path " + path.toString());
            fs.initialize(path.toUri(), conf);

            inputStream = fs.open(path, blockSize);
          }

          // Cache the data
          // Ue RRRC directly instead of creating instance of CachingFS as in certain circumstances, CachingFS could
          // send this request to NonLocalRRC which would be wrong as that would not cache it on disk
          long expectedBytesToRead = (readStart + blockSize) > fileSize ? (fileSize - readStart) : blockSize;
          RemoteReadRequestChain remoteReadRequestChain = new RemoteReadRequestChain(inputStream, localPath, byteBuffer, buffer, new BookKeeperFactory(this));
          remoteReadRequestChain.addReadRequest(new ReadRequest(readStart, readStart + blockSize, readStart, readStart + blockSize, buffer, 0, fileSize));
          remoteReadRequestChain.lock();
          Integer dataRead = remoteReadRequestChain.call();

          // Making sure the data downloaded matches with the expected bytes. If not, there is some problem with
          // the download this time. So won't update the cache metadata and return false so that client can
          // fall back on the directread
          if (dataRead == expectedBytesToRead) {
            remoteReadRequestChain.updateCacheStatus(remotePath, fileSize, lastModified, blockSize, conf);
          }
          else {
            log.error("Not able to download requested bytes. Not updating the cache for block " + startBlock);
            return false;
          }
        }
      }
      return true;
    }
    catch (Exception e) {
      log.warn("Could not cache data: " + Throwables.getStackTraceAsString(e));
      return false;
    }
    finally {
      if (inputStream != null) {
        try {
          inputStream.close();
          fs.close();
        }
        catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
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
    log.info("total free space " + avail + "MB");

    // In corner cases evictions might not make enough space for new entries
    // To minimize those cases, consider available space lower than actual
    final long total = (long) (0.95 * avail);

    fileMetadataCache = CacheBuilder.newBuilder()
        .weigher(new Weigher<String, FileMetadata>()
        {
          @Override
          public int weigh(String key, FileMetadata md)
          {
            return md.getWeight(conf);
          }
        })
        .maximumWeight((long) (total * 1.0 * CacheConfig.getCacheDataFullnessPercentage(conf) / 100.0))
        .expireAfterWrite(CacheConfig.getCacheDataExpirationAfterWrite(conf), TimeUnit.SECONDS)
        .removalListener(new RemovalListener<String, FileMetadata>()
        {
          public void onRemoval(final RemovalNotification<String, FileMetadata> notification)
          {
            FileMetadata md = notification.getValue();
            try {
              md.closeAndCleanup(notification.getCause(), fileMetadataCache);
            }
            catch (IOException e) {
              log.warn("Could not cleanup FileMetadata for " + notification.getKey(), e);
            }
          }
        })
        .build();
  }

  public void invalidateEntry(String key)
  {
    fileMetadataCache.invalidate(key);
  }

  public FileMetadata getEntry(String key, Callable<FileMetadata> callable) throws ExecutionException
  {
    return fileMetadataCache.get(key, callable);
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
