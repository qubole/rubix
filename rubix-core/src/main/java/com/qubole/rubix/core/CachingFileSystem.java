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
package com.qubole.rubix.core;

import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.qubole.rubix.common.metrics.CustomMetricsReporterProvider;
import com.qubole.rubix.spi.BookKeeperFactory;
import com.qubole.rubix.spi.CacheConfig;
import com.qubole.rubix.spi.ClusterManager;
import com.qubole.rubix.spi.ClusterType;
import com.qubole.rubix.spi.thrift.BookKeeperService;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.BufferedFSInputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.weakref.jmx.MBeanExporter;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.net.URI;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkState;
import static com.qubole.rubix.common.utils.ClusterUtil.applyRubixSiteConfig;
import static com.qubole.rubix.spi.CacheUtil.skipCache;

/**
 * Created by stagra on 29/12/15.
 */
public abstract class CachingFileSystem<T extends FileSystem> extends FilterFileSystem
{
  private static final Log log = LogFactory.getLog(CachingFileSystem.class);
  private static volatile BookKeeperFactory bookKeeperFactory = new BookKeeperFactory();

  // statics that need to initialized after the configuration object is available
  private static final AtomicBoolean initialized = new AtomicBoolean(false);
  private static volatile String statsMBeanName = "rubix:name=stats";
  private static volatile String detailedStatsMBeanName = "rubix:name=stats,type=detailed";
  private static volatile ClusterManager clusterManager;
  private static volatile CachingFileSystemStatsProvider stats;

  private boolean isRubixSchemeUsed;
  private URI uri;
  private Path workingDir;
  private long splitSize;

  private static void initialize(Configuration conf, ClusterType clusterType)
          throws IOException
  {
    if (initialized.get()) {
      return;
    }

    synchronized (initialized) {
      if (initialized.get()) {
        return;
      }

      if (!CacheConfig.isEmbeddedModeEnabled(conf)) {
        // do not initialize stats automatically in embedded mode.
        // it will be done in setLocalBookKeeper
        initializeStats(conf);
      }
      try {
        initializeClusterManager(conf, clusterType);
      } catch (ClusterManagerInitilizationException e) {
        throw new IOException(e);
      }

      initialized.set(true);
    }
  }

  private static void initializeStats(Configuration conf)
  {
    MBeanExporter exporter = new MBeanExporter(ManagementFactory.getPlatformMBeanServer());
    stats = new CachingFileSystemStatsProvider();
    try {
      if (ManagementFactory.getPlatformMBeanServer().isRegistered(new ObjectName(statsMBeanName))) {
        exporter.unexport(statsMBeanName);
      }
      if (!ManagementFactory.getPlatformMBeanServer().isRegistered(new ObjectName(statsMBeanName))) {
        exporter.export(statsMBeanName, new BasicCachingFileSystemStats(stats, bookKeeperFactory, conf));
      }

      if (ManagementFactory.getPlatformMBeanServer().isRegistered(new ObjectName(detailedStatsMBeanName))) {
        exporter.unexport(detailedStatsMBeanName);
      }
      if (!ManagementFactory.getPlatformMBeanServer().isRegistered(new ObjectName(detailedStatsMBeanName))) {
        exporter.export(detailedStatsMBeanName, new DetailedCachingFileSystemStats(stats, bookKeeperFactory, conf));
      }
    }
    catch (MalformedObjectNameException e) {
      throw new RuntimeException("Could not load MBean", e);
    }
  }

  private static void initializeClusterManager(Configuration conf, ClusterType clusterType)
          throws ClusterManagerInitilizationException
  {
    if (clusterManager != null) {
      return;
    }

    String clusterManagerClassName = CacheConfig.getClusterManagerClass(conf, clusterType);
    log.debug("Initializing cluster manager : " + clusterManagerClassName);

    try {
      Class clusterManagerClass = conf.getClassByName(clusterManagerClassName);
      Constructor constructor = clusterManagerClass.getConstructor();
      ClusterManager manager = (ClusterManager) constructor.newInstance();

      manager.initialize(conf);
      clusterManager = manager;
    }
    catch (Exception ex) {
      String errorMessage = "Not able to initialize ClusterManager class: " + clusterManagerClassName;
      log.error(errorMessage, ex);
      throw new ClusterManagerInitilizationException(errorMessage, ex);
    }
  }

  // this magic is necessary to create an instance of type T
  @SuppressWarnings("unchecked")
  private Class<T> getTypeParameterClass()
  {
    Type type = getClass().getGenericSuperclass();
    ParameterizedType paramType = (ParameterizedType) type;
    return (Class<T>) paramType.getActualTypeArguments()[0];
  }

  public CachingFileSystem()
  {
    try {
      this.fs = getTypeParameterClass().newInstance();
    }
    catch (InstantiationException | IllegalAccessException e) {
      log.error("cannot instantiate base filesystem ", e);
      Throwables.propagate(e);
    }
  }

  public FileSystem getRemoteFileSystem()
  {
    return fs;
  }

  public static void setLocalBookKeeper(Configuration configuration, BookKeeperService.Iface bookKeeper, String statsMbeanSuffix)
  {
    checkState(!initialized.get(), "LocalBookKeeper should be set before opening up the Filesystem to clients");
    bookKeeperFactory = new BookKeeperFactory(bookKeeper);
    if (!Strings.isNullOrEmpty(statsMbeanSuffix)) {
      statsMBeanName = statsMBeanName + "," + statsMbeanSuffix;
      detailedStatsMBeanName = detailedStatsMBeanName + "," + statsMbeanSuffix;
    }
    initializeStats(configuration);
  }

  public abstract String getScheme();

  @Override
  public void initialize(URI uri, Configuration conf) throws IOException
  {
    conf = applyRubixSiteConfig(conf);
    initialize(conf, getClusterType());
    super.initialize(getOriginalURI(uri), conf);
    CustomMetricsReporterProvider.initialize(conf);
    this.uri = URI.create(uri.getScheme() + "://" + uri.getAuthority());
    this.workingDir = new Path("/user", System.getProperty("user.name")).makeQualified(this);
    isRubixSchemeUsed = uri.getScheme().equals(CacheConfig.RUBIX_SCHEME);
    this.splitSize = CacheConfig.getCacheFileSplitSize(conf);
  }

  public abstract ClusterType getClusterType();

  @Override
  public URI getUri()
  {
    return this.uri;
  }

  @Override
  public FSDataInputStream open(Path path, int bufferSize)
      throws IOException
  {
    if (skipCache(path.toString(), getConf())) {
      return fs.open(path, bufferSize);
    }

    Path originalPath = new Path(getOriginalURI(path.toUri()).getScheme(), path.toUri().getAuthority(),
        path.toUri().getPath());

    if (CacheConfig.isDummyModeEnabled(this.getConf())) {
      return new FSDataInputStream(
              new BufferedFSInputStream(
                      new DummyModeCachingInputStream(this, originalPath, this.getConf(), stats,
                              clusterManager.getClusterType(), bookKeeperFactory, fs, bufferSize, statistics),
                      CacheConfig.getBlockSize(getConf())));
    }

    try {
      return new FSDataInputStream(
              new BufferedFSInputStream(
                      new CachingInputStream(originalPath, this.getConf(), stats,
                              clusterManager.getClusterType(), bookKeeperFactory, fs, bufferSize, statistics),
                      CacheConfig.getBlockSize(getConf())));
    }
    catch (Exception e) {
      if (CacheConfig.isStrictMode(this.getConf())) {
          log.error("Error in opening Caching Input Stream", e);
          throw e;
      }
      log.warn("Error in opening Caching Input Stream, skipping cache", e);
      return fs.open(path, bufferSize);
    }
  }

  @Override
  public FSDataOutputStream create(Path path, FsPermission fsPermission, boolean b, int i, short i1, long l, Progressable progressable)
      throws IOException
  {
    //CachingInputStream.invalidate(path);
    return fs.create(path, fsPermission, b, i, i1, l, progressable);
  }

  @Override
  public FSDataOutputStream append(Path path, int i, Progressable progressable)
      throws IOException
  {
    //CachingInputStream.invalidate(path);
    return fs.append(path, i, progressable);
  }

  @Override
  public boolean rename(Path path, Path path1)
      throws IOException
  {
    //CachingInputStream.invalidate(path);
    //CachingInputStream.invalidate(path1);
    return fs.rename(path, path1);
  }

  @Override
  public boolean delete(Path path)
      throws IOException
  {
    // TODO: Support directory invalidation
    // When we do support caching file listings, we should invalidate that cache here
    // Invalidation in that cache will be responsible to call CachingInputStream.invalidate()
    // for each file listing in the path
    //CachingInputStream.invalidate(path);
    return fs.delete(path);
  }

  @Override
  public boolean delete(Path path, boolean b)
      throws IOException
  {
    // TODO: Support directory invalidation, same as in delete(Path)
    //CachingInputStream.invalidate(path);
    return fs.delete(path, b);
  }

  @Override
  public FileStatus[] listStatus(Path path)
      throws FileNotFoundException, IOException
  {
    FileStatus[] files = fs.listStatus(path);
    for (int i = 0; i < files.length; i++) {
      files[i].setPath(getRubixPath(files[i].getPath(), isRubixSchemeUsed));
    }
    return files;
  }

  @Override
  public void setWorkingDirectory(Path path)
  {
    this.workingDir = path;
  }

  @Override
  public Path getWorkingDirectory()
  {
    return this.workingDir;
  }

  @Override
  public boolean mkdirs(Path path, FsPermission fsPermission)
      throws IOException
  {
    return fs.mkdirs(path, fsPermission);
  }

  @Override
  public FileStatus getFileStatus(Path path)
      throws IOException
  {
    FileStatus originalStatus = fs.getFileStatus(path);
    originalStatus.setPath(getRubixPath(originalStatus.getPath(), isRubixSchemeUsed));
    return originalStatus;
  }

  private Path getRubixPath(Path remotePath, boolean isRubixSchemeUsed)
  {
    String remotePathScheme = remotePath.toUri().getScheme();
    if (remotePathScheme.equals(getScheme()) && isRubixSchemeUsed) {
      return new Path(CacheConfig.RUBIX_SCHEME, remotePath.toUri().getAuthority(), remotePath.toUri().getPath());
    }
    return remotePath;
  }

  private URI getOriginalURI(URI actualURI)
  {
    String actualScheme = actualURI.getScheme();
    if (!CacheConfig.RUBIX_SCHEME.equals(actualScheme)) {
      return actualURI;
    }
    return URI.create(getScheme() + "://" + actualURI.getAuthority());
  }

  @Override
  public RemoteIterator<LocatedFileStatus> listLocatedStatus(Path path)
          throws IOException
  {
      return new RemoteIterator<LocatedFileStatus>()
      {
          private final RemoteIterator<LocatedFileStatus> stats = fs.listLocatedStatus(path);

          @Override
          public boolean hasNext()
                  throws IOException
          {
              return stats.hasNext();
          }

          @Override
          public LocatedFileStatus next()
                  throws IOException
          {
              LocatedFileStatus status = stats.next();
              // use caching locations explicitly
              BlockLocation[] locations = status.isFile() ? getFileBlockLocations(status, 0, status.getLen()) : null;
              return new LocatedFileStatus(status, locations);
          }
      };
  }

  @Override
  public BlockLocation[] getFileBlockLocations(FileStatus file, long start, long len) throws IOException
  {
    if (file == null) {
      return null;
    }

    if ((CacheConfig.isEmbeddedModeEnabled(getConf()) && !bookKeeperFactory.isBookKeeperInitialized())
            || skipCache(file.getPath().toString(), getConf())) {
      return fs.getFileBlockLocations(file, start, len);
    }

    List<String> nodes = clusterManager.getNodes();

    if (nodes == null) {
      return fs.getFileBlockLocations(file, start, len);
    }
    else if (start >= 0L && len >= 0L) {
      if (file.getLen() < start) {
        return new BlockLocation[0];
      }
      else {
        // Using similar logic of returning all Blocks as FileSystem.getFileBlockLocations does instead of only returning blocks from start till len

        BlockLocation[] blockLocations = new BlockLocation[(int) Math.ceil((double) file.getLen() / splitSize)];
        int blockNumber = 0;
        for (long i = 0; i < file.getLen(); i = i + splitSize) {
          long end = i + splitSize;
          if (end > file.getLen()) {
            end = file.getLen();
          }
          String key = file.getPath().toString() + i + end;
          int nodeIndex = clusterManager.getNodeIndex(nodes.size(), key);
          String[] name = new String[]{nodes.get(nodeIndex)};
          String[] host = new String[]{nodes.get(nodeIndex)};
          blockLocations[blockNumber++] = new BlockLocation(name, host, i, end - i);
          log.debug(String.format("BlockLocation %s %d %d %s totalHosts: %s", file.getPath().toString(), i, end - i, host[0], nodes.size()));
        }

        return blockLocations;
      }
    }
    else {
      throw new IllegalArgumentException("Invalid start or len parameter");
    }
  }
}
