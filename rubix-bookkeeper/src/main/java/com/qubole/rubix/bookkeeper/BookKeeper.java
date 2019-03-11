/**
 * Copyright (c) 2019. Qubole Inc
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

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.base.Ticker;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.cache.Weigher;
import com.google.common.collect.ImmutableMap;
import com.qubole.rubix.bookkeeper.utils.DiskUtils;
import com.qubole.rubix.bookkeeper.validation.CachingValidator;
import com.qubole.rubix.common.metrics.BookKeeperMetrics;
import com.qubole.rubix.core.ClusterManagerInitilizationException;
import com.qubole.rubix.core.ReadRequest;
import com.qubole.rubix.core.RemoteReadRequestChain;
import com.qubole.rubix.spi.BookKeeperFactory;
import com.qubole.rubix.spi.CacheConfig;
import com.qubole.rubix.spi.CacheUtil;
import com.qubole.rubix.spi.ClusterManager;
import com.qubole.rubix.spi.ClusterType;
import com.qubole.rubix.spi.thrift.BlockLocation;
import com.qubole.rubix.spi.thrift.BookKeeperService;
import com.qubole.rubix.spi.thrift.CacheStatusRequest;
import com.qubole.rubix.spi.thrift.FileInfo;
import com.qubole.rubix.spi.thrift.Location;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.thrift.shaded.TException;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.qubole.rubix.spi.ClusterType.TEST_CLUSTER_MANAGER;
import static com.qubole.rubix.spi.ClusterType.TEST_CLUSTER_MANAGER_MULTINODE;

/**
 * Created by stagra on 12/2/16.
 */
public abstract class BookKeeper implements BookKeeperService.Iface
{
  private static Log log = LogFactory.getLog(BookKeeper.class);

  protected static Cache<String, FileMetadata> fileMetadataCache;
  private static LoadingCache<String, FileInfo> fileInfoCache;
  protected static ClusterManager clusterManager;
  String nodeName;
  static String nodeHostName;
  static String nodeHostAddress;
  private final Configuration conf;
  private static Integer lock = 1;
  private List<String> nodes;
  int currentNodeIndex = -1;
  static long splitSize;
  private final RemoteFetchProcessor fetchProcessor;
  private final Ticker ticker;

  // Registry for gathering & storing necessary metrics
  protected final MetricRegistry metrics;

  // Metrics to keep track of cache interactions
  private static Counter cacheEvictionCount;
  private static Counter cacheInvalidationCount;
  private static Counter cacheExpiryCount;
  private Counter totalRequestCount;
  private Counter remoteRequestCount;
  private Counter cacheRequestCount;
  private Counter nonlocalRequestCount;

  public BookKeeper(Configuration conf, MetricRegistry metrics) throws FileNotFoundException
  {
    this(conf, metrics, Ticker.systemTicker());
  }

  @Override
  public boolean isBookKeeperAlive()
  {
    return true;
  }

  @VisibleForTesting
  BookKeeper(Configuration conf, MetricRegistry metrics, Ticker ticker) throws FileNotFoundException
  {
    this.conf = conf;
    this.metrics = metrics;
    this.ticker = ticker;
    initializeMetrics();
    initializeCache(conf, ticker);
    cleanupOldCacheFiles(conf);
    fetchProcessor = new RemoteFetchProcessor(this, metrics, conf);
    fetchProcessor.startAsync();
  }

  RemoteFetchProcessor getRemoteFetchProcessorInstance()
  {
    return fetchProcessor;
  }

  // Cleanup the cached files that were downloaded as a part of previous bookkeeper session.
  // This makes sure we always start with a clean empty cash.
  // TODO: We need to come up with a way to persist the files being downloaded before
  // So that we can use that info to load those files in guava cache.
  private void cleanupOldCacheFiles(Configuration conf)
  {
    if (CacheConfig.isCleanupFilesDuringStartEnabled(conf)) {
      try {
        int numDisks = CacheConfig.getCacheMaxDisks(conf);
        String dirSuffix = CacheConfig.getCacheDataDirSuffix(conf);
        List<String> dirPrefixList = CacheUtil.getDirPrefixList(conf);

        for (String dirPrefix : dirPrefixList) {
          for (int i = 0; i < numDisks; i++) {
            java.nio.file.Path path = Paths.get(dirPrefix + i, dirSuffix, "*");
            DiskUtils.clearDirectory(path.toString());
          }
        }
      }
      catch (IOException ex) {
        log.error("Could not clean up the old cached files");
      }
    }
  }

  /**
   * Initialize the instruments used for gathering desired metrics.
   */
  private void initializeMetrics()
  {
    cacheEvictionCount = metrics.counter(BookKeeperMetrics.CacheMetric.CACHE_EVICTION_COUNT.getMetricName());
    cacheInvalidationCount = metrics.counter(BookKeeperMetrics.CacheMetric.CACHE_INVALIDATION_COUNT.getMetricName());
    cacheExpiryCount = metrics.counter(BookKeeperMetrics.CacheMetric.CACHE_EXPIRY_COUNT.getMetricName());
    totalRequestCount = metrics.counter(BookKeeperMetrics.CacheMetric.TOTAL_REQUEST_COUNT.getMetricName());
    cacheRequestCount = metrics.counter(BookKeeperMetrics.CacheMetric.CACHE_REQUEST_COUNT.getMetricName());
    nonlocalRequestCount = metrics.counter(BookKeeperMetrics.CacheMetric.NONLOCAL_REQUEST_COUNT.getMetricName());
    remoteRequestCount = metrics.counter(BookKeeperMetrics.CacheMetric.REMOTE_REQUEST_COUNT.getMetricName());

    metrics.register(BookKeeperMetrics.CacheMetric.CACHE_HIT_RATE_GAUGE.getMetricName(), new Gauge<Double>()
    {
      @Override
      public Double getValue()
      {
        return ((double) cacheRequestCount.getCount() / (cacheRequestCount.getCount() + remoteRequestCount.getCount()));
      }
    });
    metrics.register(BookKeeperMetrics.CacheMetric.CACHE_MISS_RATE_GAUGE.getMetricName(), new Gauge<Double>()
    {
      @Override
      public Double getValue()
      {
        return ((double) remoteRequestCount.getCount() / (cacheRequestCount.getCount() + remoteRequestCount.getCount()));
      }
    });
    metrics.register(BookKeeperMetrics.CacheMetric.CACHE_SIZE_GAUGE.getMetricName(), new Gauge<Integer>()
    {
      @Override
      public Integer getValue()
      {
        return DiskUtils.getCacheSizeMB(conf);
      }
    });
  }

  @Override
  public List<BlockLocation> getCacheStatus(CacheStatusRequest request) throws TException
  {
    try {
      initializeClusterManager(request.getClusterType());
    }
    catch (ClusterManagerInitilizationException ex) {
      log.error("Not able to initialize ClusterManager for cluster type : " +
          ClusterType.findByValue(request.getClusterType()) + " with Exception : " + ex);
      return null;
    }
    if (nodeName == null) {
      log.error("Node name is null for Cluster Type" + ClusterType.findByValue(request.getClusterType()));
      return null;
    }

    if (currentNodeIndex == -1 || nodes == null) {
      log.error("Initialization not done");
      return null;
    }

    Map<Long, String> blockSplits = new HashMap<>();
    long blockNumber = 0;

    long fileLength = request.getFileLength();
    String remotePath = request.getRemotePath();
    long lastModified = request.getLastModified();
    long startBlock = request.getStartBlock();
    long endBlock = request.getEndBlock();
    boolean incrMetrics = request.isIncrMetrics();

    for (long i = 0; i < fileLength; i = i + splitSize) {
      long end = i + splitSize;
      if (end > fileLength) {
        end = fileLength;
      }
      String key = remotePath + i + end;
      int nodeIndex = clusterManager.getNodeIndex(nodes.size(), key);
      blockSplits.put(blockNumber, nodes.get(nodeIndex));
      blockNumber++;
    }

    FileMetadata md;
    try {
      md = fileMetadataCache.get(remotePath, new CreateFileMetadataCallable(remotePath, fileLength, lastModified, 0, conf));
      if (isInvalidationRequired(md.getLastModified(), lastModified)) {
        invalidateFileMetadata(remotePath);
        md = fileMetadataCache.get(remotePath, new CreateFileMetadataCallable(remotePath, fileLength, lastModified, 0, conf));
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
    int totalRequests = 0;
    int cacheRequests = 0;
    int remoteRequests = 0;
    int nonLocalRequests = 0;

    try {
      for (long blockNum = startBlock; blockNum < endBlock; blockNum++) {
        totalRequests++;

        long split = (blockNum * blockSize) / splitSize;
        if (!blockSplits.get(split).equalsIgnoreCase(nodeName)) {
          blockLocations.add(new BlockLocation(Location.NON_LOCAL, blockSplits.get(split)));
          nonLocalRequests++;
        }
        else {
          if (md.isBlockCached(blockNum)) {
            blockLocations.add(new BlockLocation(Location.CACHED, blockSplits.get(split)));
            cacheRequests++;
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

    if (request.isIncrMetrics() && !isValidatingCachingBehavior(remotePath)) {
      totalRequestCount.inc(totalRequests);
      nonlocalRequestCount.inc(nonLocalRequests);
      cacheRequestCount.inc(cacheRequests);
      remoteRequestCount.inc(remoteRequests);
    }

    return blockLocations;
  }

  private void initializeClusterManager(int clusterType) throws ClusterManagerInitilizationException
  {
    if (this.clusterManager == null) {
      ClusterManager manager = null;
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

          manager = getClusterManagerInstance(ClusterType.findByValue(clusterType), conf);
          manager.initialize(conf);
          this.clusterManager = manager;
          splitSize = clusterManager.getSplitSize();

          if (clusterType == TEST_CLUSTER_MANAGER.ordinal() || clusterType == TEST_CLUSTER_MANAGER_MULTINODE.ordinal()) {
            currentNodeIndex = 0;
            nodes = clusterManager.getNodes();
            nodeName = nodes.get(currentNodeIndex);
            if (clusterType == TEST_CLUSTER_MANAGER_MULTINODE.ordinal()) {
              nodes.add(nodeName + "_copy");
            }
            return;
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

  @VisibleForTesting
  public ClusterManager getClusterManagerInstance(ClusterType clusterType, Configuration config)
      throws ClusterManagerInitilizationException
  {
    String clusterManagerClassName = CacheConfig.getClusterManagerClass(conf, clusterType);
    log.info("Initializing cluster manager : " + clusterManagerClassName);
    ClusterManager manager = null;

    try {
      Class clusterManagerClass = conf.getClassByName(clusterManagerClassName);
      Constructor constructor = clusterManagerClass.getConstructor();
      manager = (ClusterManager) constructor.newInstance();
    }
    catch (ClassNotFoundException | NoSuchMethodException | InstantiationException |
        IllegalAccessException | InvocationTargetException ex) {
      String errorMessage = String.format("Not able to initialize ClusterManager class : {0} ",
          clusterManagerClassName);
      log.error(errorMessage);
      throw new ClusterManagerInitilizationException(errorMessage, ex);
    }

    return manager;
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
      log.info(String.format("Could not update the metadata for file %s", remotePath));
      return;
    }
    if (isInvalidationRequired(md.getLastModified(), lastModified)) {
      invalidateFileMetadata(remotePath);
      return;
    }
    endBlock = setCorrectEndBlock(endBlock, fileLength, remotePath);
    log.debug("Updating cache for " + remotePath + " StarBlock : " + startBlock + " EndBlock : " + endBlock);

    try {
      md.setBlocksCached(startBlock, endBlock);
      long currentFileSize = md.incrementCurrentFileSize((endBlock - startBlock) * CacheConfig.getBlockSize(conf));
      replaceFileMetadata(remotePath, currentFileSize, conf);
    }
    catch (IOException e) {
      throw new TException(e);
    }
  }

  @Override
  public Map<String, Double> getCacheMetrics()
  {
    final long cachedRequests = cacheRequestCount.getCount();
    final long remoteRequests = remoteRequestCount.getCount();
    final long nonLocalRequests = nonlocalRequestCount.getCount();
    final long totalRequests = totalRequestCount.getCount();

    ImmutableMap.Builder<String, Double> cacheMetrics = ImmutableMap.builder();
    cacheMetrics.put(BookKeeperMetrics.CacheMetric.CACHE_HIT_RATE_GAUGE.getMetricName(), ((double) cachedRequests / (cachedRequests + remoteRequests)));
    cacheMetrics.put(BookKeeperMetrics.CacheMetric.CACHE_MISS_RATE_GAUGE.getMetricName(), ((double) (remoteRequests) / (cachedRequests + remoteRequests)));
    cacheMetrics.put(BookKeeperMetrics.CacheMetric.CACHE_REQUEST_COUNT.getMetricName(), ((double) cachedRequests));
    cacheMetrics.put(BookKeeperMetrics.CacheMetric.REMOTE_REQUEST_COUNT.getMetricName(), ((double) remoteRequests));
    cacheMetrics.put(BookKeeperMetrics.CacheMetric.NONLOCAL_REQUEST_COUNT.getMetricName(), ((double) nonLocalRequests));
    cacheMetrics.put(BookKeeperMetrics.CacheMetric.TOTAL_REQUEST_COUNT.getMetricName(), ((double) totalRequests));
    cacheMetrics.put(BookKeeperMetrics.CacheMetric.CACHE_EVICTION_COUNT.getMetricName(), (double) cacheEvictionCount.getCount());
    cacheMetrics.put(BookKeeperMetrics.CacheMetric.CACHE_INVALIDATION_COUNT.getMetricName(), (double) cacheInvalidationCount.getCount());
    cacheMetrics.put(BookKeeperMetrics.CacheMetric.CACHE_EXPIRY_COUNT.getMetricName(), (double) cacheExpiryCount.getCount());
    cacheMetrics.put(BookKeeperMetrics.CacheMetric.CACHE_SIZE_GAUGE.getMetricName(), (double) DiskUtils.getCacheSizeMB(conf));
    return cacheMetrics.build();
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

  @Override
  public FileInfo getFileInfo(String remotePath) throws TException
  {
    if (CacheConfig.isFileStalenessCheckEnabled(conf)) {
      try {
        Path path = new Path(remotePath);
        FileSystem fs = path.getFileSystem(conf);
        FileStatus status = fs.getFileStatus(path);
        FileInfo info = new FileInfo(status.getLen(), status.getModificationTime());
        return info;
      }
      catch (Exception e) {
        log.error(String.format("Could not fetch FileStatus from remote file system for %s : %s", remotePath, Throwables.getStackTraceAsString(e)));
      }
    }
    else {
      try {
        return fileInfoCache.get(remotePath);
      }
      catch (ExecutionException e) {
        log.error(String.format("Could not fetch FileInfo from Cache for %s : %s", remotePath, Throwables.getStackTraceAsString(e)));
        throw new TException(e);
      }
    }

    return null;
  }

  private boolean readDataInternal(String remotePath, long offset, int length, long fileSize,
                                   long lastModified, int clusterType) throws TException
  {
    int blockSize = CacheConfig.getBlockSize(conf);
    byte[] buffer = new byte[blockSize];
    ByteBuffer byteBuffer = null;
    String localPath = CacheUtil.getLocalPath(remotePath, conf);
    FileSystem fs = null;
    FSDataInputStream inputStream = null;
    Path path = new Path(remotePath);
    long startBlock = offset / blockSize;
    long endBlock = ((offset + (length - 1)) / CacheConfig.getBlockSize(conf)) + 1;
    try {
      int idx = 0;
      CacheStatusRequest request = new CacheStatusRequest(remotePath, fileSize, lastModified, startBlock, endBlock, clusterType);
      List<BlockLocation> blockLocations = getCacheStatus(request);

      for (long blockNum = startBlock; blockNum < endBlock; blockNum++, idx++) {
        long readStart = blockNum * blockSize;
        log.debug(" blockLocation is: " + blockLocations.get(idx).getLocation() + " for path " + remotePath + " offset " + offset + " length " + length);
        if (blockLocations.get(idx).getLocation() != Location.CACHED) {
          if (byteBuffer == null) {
            byteBuffer = ByteBuffer.allocateDirect(CacheConfig.getDiskReadBufferSize(conf));
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

  private static synchronized void initializeCache(final Configuration conf, final Ticker ticker)
      throws FileNotFoundException
  {
    CacheUtil.createCacheDirectories(conf);

    long avail = 0;
    for (int d = 0; d < CacheUtil.getCacheDiskCount(conf); d++) {
      avail += new File(CacheUtil.getDirPath(d, conf)).getUsableSpace();
    }
    avail = DiskUtils.bytesToMB(avail);
    log.info("total free space " + avail + "MB");

    // In corner cases evictions might not make enough space for new entries
    // To minimize those cases, consider available space lower than actual
    final long total = (long) (0.95 * avail);

    final long cacheMaxSize = CacheConfig.getCacheDataFullnessMaxSize(conf);
    final long maxWeight = (cacheMaxSize == 0)
        ? (long) (total * 1.0 * CacheConfig.getCacheDataFullnessPercentage(conf) / 100.0)
        : cacheMaxSize;

    initializeFileInfoCache(conf, ticker);

    fileMetadataCache = CacheBuilder.newBuilder()
        .ticker(ticker)
        .weigher(new Weigher<String, FileMetadata>()
        {
          @Override
          public int weigh(String key, FileMetadata md)
          {
            return md.getWeight(conf);
          }
        })
        .maximumWeight(maxWeight)
        .expireAfterWrite(CacheConfig.getCacheDataExpirationAfterWrite(conf), TimeUnit.MILLISECONDS)
        .removalListener(new CacheRemovalListener())
        .build();
  }

  public FileMetadata getEntry(String key, Callable<FileMetadata> callable) throws ExecutionException
  {
    return fileMetadataCache.get(key, callable);
  }

  private static void initializeFileInfoCache(final Configuration conf, final Ticker ticker)
  {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    int expiryPeriod = CacheConfig.getStaleFileInfoExpiryPeriod(conf);
    fileInfoCache = CacheBuilder.newBuilder()
        .ticker(ticker)
        .expireAfterWrite(expiryPeriod, TimeUnit.SECONDS)
        .removalListener(new RemovalListener<String, FileInfo>()
        {
          @Override
          public void onRemoval(RemovalNotification<String, FileInfo> notification)
          {
            log.info("Removed FileInfo for path " + notification.getKey() + " due to " + notification.getCause());
          }
        })
        .build(CacheLoader.asyncReloading(new CacheLoader<String, FileInfo>()
        {
          @Override
          public FileInfo load(String s) throws Exception
          {
            log.info("Fetching FileStatus for : " + s);
            Path path = new Path(s);
            FileSystem fs = path.getFileSystem(conf);
            FileStatus status = fs.getFileStatus(path);
            FileInfo info = new FileInfo(status.getLen(), status.getModificationTime());
            return info;
          }
        }, executor));
  }

  protected static class CacheRemovalListener implements RemovalListener<String, FileMetadata>
  {
    @Override
    public void onRemoval(RemovalNotification<String, FileMetadata> notification)
    {
      FileMetadata md = notification.getValue();
      try {
        md.closeAndCleanup(notification.getCause(), fileMetadataCache);
        if (!isValidatingCachingBehavior(md.getRemotePath())) {
          switch (notification.getCause()) {
            case EXPLICIT:
              cacheInvalidationCount.inc();
              break;
            case SIZE:
              cacheEvictionCount.inc();
              break;
            case EXPIRED:
              cacheExpiryCount.inc();
              break;
            default:
              break;
          }
        }
      }
      catch (IOException e) {
        log.warn("Could not cleanup FileMetadata for " + notification.getKey(), e);
      }
    }
  }

  private static class CreateFileMetadataCallable
      implements Callable<FileMetadata>
  {
    String path;
    Configuration conf;
    long fileLength;
    long lastModified;
    long currentFileSize;

    public CreateFileMetadataCallable(String path, long fileLength, long lastModified, long currentFileSize,
                                      Configuration conf)
    {
      this.path = path;
      this.conf = conf;
      this.fileLength = fileLength;
      this.lastModified = lastModified;
      this.currentFileSize = currentFileSize;
    }

    public FileMetadata call()
        throws Exception
    {
      return new FileMetadata(path, fileLength, lastModified, currentFileSize, conf);
    }
  }

  // This method is to invalidate FileMetadata from guava cache.
  @Override
  public void invalidateFileMetadata(String key)
  {
    // We might come in here with cache not initialized e.g. fs.create
    if (fileMetadataCache != null) {
      FileMetadata metadata = fileMetadataCache.getIfPresent(key);
      if (metadata != null) {
        log.info("Invalidating file " + key + " from metadata cache");
        fileMetadataCache.invalidate(key);
      }
    }
  }

  private void replaceFileMetadata(String key, long currentFileSize, Configuration conf) throws IOException
  {
    if (fileMetadataCache != null) {
      FileMetadata metadata = fileMetadataCache.getIfPresent(key);
      if (metadata != null) {
        FileMetadata newMetaData = new FileMetadata(key, metadata.getFileSize(), metadata.getLastModified(),
            currentFileSize, conf);
        fileMetadataCache.put(key, newMetaData);
      }
    }
  }

  private boolean isInvalidationRequired(long metadataLastModifiedTime, long remoteLastModifiedTime)
  {
    if (CacheConfig.isFileStalenessCheckEnabled(conf) && (metadataLastModifiedTime != remoteLastModifiedTime)) {
      return true;
    }

    return false;
  }

  private static boolean isValidatingCachingBehavior(String remotePath)
  {
    return CachingValidator.VALIDATOR_TEST_FILE_NAME.equals(CacheUtil.getName(remotePath));
  }
}
