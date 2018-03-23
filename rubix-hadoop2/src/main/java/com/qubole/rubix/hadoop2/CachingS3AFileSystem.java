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
package com.qubole.rubix.hadoop2;

import com.qubole.rubix.core.CachingFileSystem;
import com.qubole.rubix.spi.ClusterManager;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.S3AFileSystem;

import java.io.IOException;
import java.net.URI;

public class CachingS3AFileSystem extends CachingFileSystem<S3AFileSystem>
{
  private static final Log LOG = LogFactory.getLog(CachingS3AFileSystem.class);
  private ClusterManager clusterManager;
  private static final String SCHEME = "s3a";

  public CachingS3AFileSystem() throws IOException
  {
    super();
  }

  @Override
  public void initialize(URI uri, Configuration conf) throws IOException
  {
    LOG.debug("Initializing CachingS3AFileSystem - Hadoop2");
    if (clusterManager == null) {
      initializeClusterManager(conf);
    }
    setClusterManager(clusterManager);
    super.initialize(uri, conf);
  }

  public String getScheme()
  {
    return SCHEME;
  }

  private synchronized void initializeClusterManager(Configuration conf)
  {
    if (clusterManager != null) {
      return;
    }
    clusterManager = new Hadoop2ClusterManager();
    clusterManager.initialize(conf);
  }
}
