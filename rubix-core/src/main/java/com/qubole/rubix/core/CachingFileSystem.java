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
package com.qubole.rubix.core;

import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.qubole.rubix.spi.BookKeeperFactory;
import com.qubole.rubix.spi.CacheConfig;
import com.qubole.rubix.spi.ClusterManager;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.BufferedFSInputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.weakref.jmx.MBeanExporter;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.net.URI;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static com.qubole.rubix.spi.CacheConfig.skipCache;

/**
 * Created by stagra on 29/12/15.
 */
public abstract class CachingFileSystem<T extends FileSystem> extends FileSystem
{
  private static final Log log = LogFactory.getLog(CachingFileSystem.class);
  protected T fs;
  private static ClusterManager clusterManager;

  private boolean cacheSkipped;
  private boolean isRubixSchemeUsed;
  private URI uri;
  private Path workingDir;

  private static CachingFileSystemStats statsMBean;
  public BookKeeperFactory bookKeeperFactory = new BookKeeperFactory();

  static {
    MBeanExporter exporter = new MBeanExporter(ManagementFactory.getPlatformMBeanServer());
    statsMBean = new CachingFileSystemStats();
    exporter.export("rubix:name=stats", statsMBean);
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

  public abstract String getScheme();

  public void setClusterManager(ClusterManager clusterManager)
  {
    this.clusterManager = clusterManager;
  }

  public void setBookKeeper(BookKeeperFactory bookKeeperFactory, Configuration conf)
  {
    this.bookKeeperFactory = bookKeeperFactory;
    this.setConf(conf);
  }

  @Override
  public void initialize(URI uri, Configuration conf) throws IOException
  {
    if (clusterManager == null) {
      throw new IOException("Cluster Manager not set");
    }
    super.initialize(uri, conf);
    this.uri = URI.create(uri.getScheme() + "://" + uri.getAuthority());
    this.workingDir = new Path("/user", System.getProperty("user.name")).makeQualified(this);
    isRubixSchemeUsed = uri.getScheme().equals(CacheConfig.RUBIX_SCHEME);
    URI originalUri = getOriginalURI(uri);
    fs.initialize(originalUri, conf);
  }

  @Override
  public URI getUri()
  {
    return this.uri;
  }

  @Override
  public FSDataInputStream open(Path path, int bufferSize)
      throws IOException
  {
    FSDataInputStream inputStream = null;

    if (skipCache(path, getConf())) {
      inputStream = fs.open(path, bufferSize);
      cacheSkipped = true;
      return inputStream;
    }

    Path originalPath = new Path(getOriginalURI(path.toUri()).getScheme(), path.toUri().getAuthority(),
        path.toUri().getPath());
    return new FSDataInputStream(
        new BufferedFSInputStream(
            new CachingInputStream(this, originalPath, this.getConf(), statsMBean,
                clusterManager.getClusterType(), bookKeeperFactory, fs, bufferSize, statistics),
            CacheConfig.getBlockSize(getConf())));
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
    if (!actualScheme.equals(CacheConfig.RUBIX_SCHEME)) {
      return actualURI;
    }
    return URI.create(getScheme() + "://" + actualURI.getAuthority());
  }

  @Override
  public BlockLocation[] getFileBlockLocations(FileStatus file, long start, long len) throws IOException
  {
    try {
      if (!clusterManager.isMaster() || cacheSkipped) {
        // If in worker node, blockLocation does not matter
        return fs.getFileBlockLocations(file, start, len);
      }
    }
    catch (ExecutionException e) {
      log.info("Could not find whether node is Master");
      return fs.getFileBlockLocations(file, start, len);
    }

    List<String> nodes = clusterManager.getNodes();

    if (nodes == null) {
      return fs.getFileBlockLocations(file, start, len);
    }

    if (file == null) {
      return null;
    }
    else if (start >= 0L && len >= 0L) {
      if (file.getLen() < start) {
        return new BlockLocation[0];
      }
      else {
        // Using similar logic of returning all Blocks as FileSystem.getFileBlockLocations does instead of only returning blocks from start till len

        BlockLocation[] blockLocations = new BlockLocation[(int) Math.ceil((double) file.getLen() / clusterManager.getSplitSize())];
        int blockNumber = 0;
        for (long i = 0; i < file.getLen(); i = i + clusterManager.getSplitSize()) {
          long end = i + clusterManager.getSplitSize();
          if (end > file.getLen()) {
            end = file.getLen();
          }
          String key = file.getPath().toString() + i + end;
          HashFunction hf = Hashing.md5();
          HashCode hc = hf.hashString(key, Charsets.UTF_8);
          int nodeIndex = Hashing.consistentHash(hc, nodes.size());
          String[] name = new String[]{nodes.get(nodeIndex)};
          String[] host = new String[]{nodes.get(nodeIndex)};
          blockLocations[blockNumber++] = new BlockLocation(name, host, i, end - i);
          log.info(String.format("BlockLocation %s %d %d %s totalHosts: %s", file.getPath().toString(), i, end - i, host[0], nodes.size()));
        }

        return blockLocations;
      }
    }
    else {
      throw new IllegalArgumentException("Invalid start or len parameter");
    }
  }
}
