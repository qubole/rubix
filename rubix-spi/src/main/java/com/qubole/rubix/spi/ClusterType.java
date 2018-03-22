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
package com.qubole.rubix.spi;

/**
 * Created by qubole on 19/9/16.
 */
public enum ClusterType
{
  HADOOP2_CLUSTER_MANAGER,
  PRESTO_CLUSTER_MANAGER,
  HADOOP_CLUSTER_MANAGER,
  TEST_CLUSTER_MANAGER;

  public static ClusterType findByValue(int value)
  {
    switch (value) {
      case 0:
        return HADOOP2_CLUSTER_MANAGER;
      case 1:
        return PRESTO_CLUSTER_MANAGER;
      case 2:
        return HADOOP_CLUSTER_MANAGER;
      case 3:
        return TEST_CLUSTER_MANAGER;
      default:
        return null;
    }
  }
}
