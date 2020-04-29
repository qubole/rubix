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

package com.qubole.rubix.bookkeeper;

import java.util.Objects;

public class FetchRequest
{
  private String remotePath;
  private long offset;
  private int length;
  private long fileSize;
  private long lastModified;
  private long requestedTime;

  FetchRequest(String remotePath, long offset, int length, long fileSize, long lastModified, long requestedTime)
  {
    this.remotePath = remotePath;
    this.offset = offset;
    this.length = length;
    this.fileSize = fileSize;
    this.lastModified = lastModified;
    this.requestedTime = requestedTime;
  }

  public String getRemotePath()
  {
    return this.remotePath;
  }

  public long getOffset()
  {
    return this.offset;
  }

  public int getLength()
  {
    return this.length;
  }

  public long getFileSize()
  {
    return this.fileSize;
  }

  public long getLastModified()
  {
    return this.lastModified;
  }

  public long getRequestedTime()
  {
    return this.requestedTime;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    FetchRequest that = (FetchRequest) o;
    return offset == that.offset &&
            length == that.length &&
            fileSize == that.fileSize &&
            lastModified == that.lastModified &&
            remotePath.equals(that.remotePath);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(remotePath, offset, length, fileSize, lastModified);
  }
}
