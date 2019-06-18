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
package com.qubole.rubix.spi;

import com.qubole.rubix.spi.thrift.ClusterNode;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import java.util.List;

/**
 * Created by stagra on 14/1/16.
 */

/*
 * This class should be implemented for each engine.
 * The implementation should return the nodes in a form which the scheduler of that engine can recognize and route the splits to
 */
public abstract class ClusterManager
{
  private static Log log = LogFactory.getLog(ClusterManager.class.getName());

  private int nodeRefreshTime;

  public ClusterType getClusterType()
  {
    return null;
  }

  public void initialize(Configuration conf)
  {
    nodeRefreshTime = CacheConfig.getClusterNodeRefreshTime(conf);
  }

  public int getNodeRefreshTime()
  {
    return nodeRefreshTime;
  }

  // Nodes format as per the note above
  // Should return sorted list
  public abstract List<ClusterNode> getNodes();
}
