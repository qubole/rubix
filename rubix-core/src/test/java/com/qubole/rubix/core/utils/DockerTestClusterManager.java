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
package com.qubole.rubix.core.utils;

import com.google.common.collect.Lists;
import com.qubole.rubix.spi.ClusterManager;
import com.qubole.rubix.spi.ClusterType;

import java.util.List;
import java.util.concurrent.ExecutionException;

public class DockerTestClusterManager extends ClusterManager
{
  @Override
  public List<String> getNodes()
  {
    return Lists.newArrayList("172.18.8.1", "172.18.8.2");
  }

  @Override
  public ClusterType getClusterType()
  {
    return ClusterType.TEST_CLUSTER_MANAGER;
  }

  @Override
  public boolean isMaster() throws ExecutionException
  {
    return false;
  }

  @Override
  public Integer getNextRunningNodeIndex(int startIndex)
  {
    return startIndex;
  }

  @Override
  public Integer getPreviousRunningNodeIndex(int startIndex)
  {
    return startIndex;
  }
}
