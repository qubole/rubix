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
 * Created by qubole on 21/11/16.
 */
public class DataTransferHeader
{
  public long offset;
  public int readLength;
  public long fileSize;
  public long lastModified;
  public int clusterType;
  public int filePathLength;
  public String filePath;

  public DataTransferHeader(long offset, int readLength, long fileSize,
                            long lastModified, int clusterType, String filePath)
  {
    this.clusterType = clusterType;
    this.filePath = filePath;
    this.fileSize = fileSize;
    this.lastModified = lastModified;
    this.offset = offset;
    this.filePathLength = filePath.length();
    this.readLength = readLength;
  }

  public int getClusterType()
  {
    return clusterType;
  }

  public long getOffset()
  {
    return offset;
  }

  public int getReadLength()
  {
    return readLength;
  }

  public long getFileSize()
  {
    return fileSize;
  }

  public long getLastModified()
  {
    return lastModified;
  }

  public int getFilePathLength()
  {
    return filePathLength;
  }

  public String getFilePath()
  {
    return filePath;
  }
}
