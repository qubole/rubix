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
package com.qubole.rubix.client.robotframework.driver.client;

import java.io.Serializable;

public class ReadDataRequestParams implements Serializable
{
  private final String remotePath;
  private final long readStart;
  private final int length;
  private final long fileSize;
  private final long lastModified;
  private final int clusterType;

  public ReadDataRequestParams(String remotePath, long readStart, int length, long fileSize, long lastModified, int clusterType)
  {
    this.remotePath = remotePath;
    this.readStart = readStart;
    this.length = length;
    this.fileSize = fileSize;
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

  public int getLength()
  {
    return length;
  }

  public long getFileSize()
  {
    return fileSize;
  }

  public long getLastModified()
  {
    return lastModified;
  }

  public int getClusterType()
  {
    return clusterType;
  }
}
