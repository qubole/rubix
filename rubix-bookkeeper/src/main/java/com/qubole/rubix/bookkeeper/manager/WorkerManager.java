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

package com.qubole.rubix.bookkeeper.manager;

import com.qubole.rubix.bookkeeper.HeartbeatService;
import org.apache.hadoop.conf.Configuration;

/**
 * Class to manage components on a worker node.
 */
public class WorkerManager
{
  // The current Hadoop configuration.
  private final Configuration conf;

  public WorkerManager(Configuration conf)
  {
    this.conf = conf;
  }

  /**
   * Start the {@link HeartbeatService} for this worker node.
   */
  public void startHeartbeatService()
  {
    HeartbeatService heartbeatService = new HeartbeatService(conf);
    heartbeatService.startAsync();
  }
}
