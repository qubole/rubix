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

import com.qubole.rubix.spi.CacheConfig;
import com.qubole.rubix.spi.ClusterType;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BufferedFSInputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;

import java.io.File;
import java.io.IOException;
import java.net.URI;

/**
 * Created by sakshia on 25/11/16.
 */

public class MockCachingFileSystem extends CachingFileSystem<RawLocalFileSystem>
{
  private static final Log log = LogFactory.getLog(MockCachingFileSystem.class);
  Configuration conf;
  private static final String SCHEME = "file";

  @Override
  public void initialize(URI uri, Configuration conf) throws IOException
  {
    this.conf = conf;
    try {
      initializeClusterManager(conf, ClusterType.TEST_CLUSTER_MANAGER);
      super.initialize(uri, conf);
    }
    catch (ClusterManagerInitilizationException ex) {
      throw new IOException(ex);
    }
  }

  public String getScheme()
  {
    return SCHEME;
  }

  @Override
  public FSDataInputStream open(Path path, int i)
      throws IOException
  {
    String localPath = path.toString();
    File file = new File(localPath);
    LocalFSInputStream inputStream = new LocalFSInputStream(localPath);
    return new FSDataInputStream(
        new BufferedFSInputStream(
            new CachingInputStream(new FSDataInputStream(inputStream), conf, path, file.length(),
                file.lastModified(), new CachingFileSystemStats(),
                ClusterType.TEST_CLUSTER_MANAGER, bookKeeperFactory, fs,
                CacheConfig.getBlockSize(conf), statistics),
            CacheConfig.getBlockSize(conf)));
  }

  @Override
  public FSDataInputStream open(Path path) throws IOException
  {
    FSDataInputStream stream = fs.open(path, CacheConfig.getBlockSize(conf));
    return stream;
  }
}
