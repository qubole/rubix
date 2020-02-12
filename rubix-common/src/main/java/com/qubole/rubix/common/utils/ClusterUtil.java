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

package com.qubole.rubix.common.utils;

import com.qubole.rubix.spi.CacheConfig;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

public class ClusterUtil
{
  private static final Log log = LogFactory.getLog(ClusterUtil.class.getName());

  private ClusterUtil()
  {
  }

  /**
   * Get the hostname for the master node in the cluster.
   *
   * @return The hostname of the master node, or "localhost" if the hostname could not be found.
   */
  public static String getMasterHostname(Configuration conf)
  {
    log.debug("Trying master.hostname");
    String host = CacheConfig.getCoordinatorHostName(conf);
    if (host != null) {
      return host;
    }

    log.debug("Trying yarn.resourcemanager.address");
    host = CacheConfig.getResourceManagerAddress(conf);
    if (host != null) {
      return host.contains(":")
          ? host.substring(0, host.indexOf(":"))
          : host;
    }

    log.debug("No hostname found in etc/*-site.xml, returning localhost");
    return "localhost";
  }
}
