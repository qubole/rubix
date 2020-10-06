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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Set;

public abstract class SyncClusterManager extends ClusterManager
{
  private static Log log = LogFactory.getLog(SyncClusterManager.class);

  private Set<String> currentNodes;

  protected abstract boolean hasStateChanged();

  private synchronized void updateStateIfChanged() {
    if (hasStateChanged()) {
      currentNodes = getNodesAndUpdateState();
    }
  }

  @Override
  public synchronized String locateKey(String key)
  {
    updateStateIfChanged();
    return super.locateKey(key);
  }

  @Override
  public synchronized String getCurrentNodeName()
  {
    updateStateIfChanged();
    return super.getCurrentNodeName();
  }
  // Returns sorted list of nodes in the cluster
  @Override
  public synchronized Set<String> getNodes()
  {
    updateStateIfChanged();
    return currentNodes;
  }
}
