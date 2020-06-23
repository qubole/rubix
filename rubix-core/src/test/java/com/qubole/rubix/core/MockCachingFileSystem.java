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

import com.qubole.rubix.spi.ClusterType;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
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
  public ClusterType getClusterType() {
    return ClusterType.TEST_CLUSTER_MANAGER;
  }

  public String getScheme()
  {
    return SCHEME;
  }
}
