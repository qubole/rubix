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
package com.qubole.rubix.client.robotframework;

import com.qubole.rubix.spi.ClusterType;

public class TestClientReadRequest
{
  private final String remotePath;
  private final long readStart;
  private final int readLength;
  private final long fileLength;
  private final long lastModified;
  private final int clusterType;

  public TestClientReadRequest(String remotePath,
                               long readStart,
                               int readLength,
                               long fileLength,
                               long lastModified,
                               int clusterType)
  {
    this.remotePath = remotePath;
    this.readStart = readStart;
    this.readLength = readLength;
    this.fileLength = fileLength;
    this.lastModified = lastModified;
    this.clusterType = clusterType;
  }

  public String getRemotePath()
  {
    return remotePath;
  }

  public long getReadStart()
  {
    return readStart;
  }

  public int getReadLength()
  {
    return readLength;
  }

  public long getFileLength()
  {
    return fileLength;
  }

  public long getLastModified()
  {
    return lastModified;
  }

  public int getClusterType()
  {
    return clusterType;
  }

  @Override
  public String toString()
  {
    return String.format(
        "Request for file %s [%s-%s] (%sB / LM: %s) {Cluster type: %s}",
        remotePath,
        readStart,
        readLength,
        fileLength,
        lastModified,
        ClusterType.findByValue(clusterType));
  }
}
