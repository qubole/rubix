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

package com.qubole.rubix.bookkeeper;

import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;

/**
 * Created by Abhishek on 3/8/18.
 */
public class DownloadRequestContext
{
  private String remoteFilePath;
  private long fileSize;
  private long lastModified;
  RangeSet<Long> rangeSet;

  public DownloadRequestContext(String remoteFilePath, long fileSize, long lastModified)
  {
    this.remoteFilePath = remoteFilePath;
    this.fileSize = fileSize;
    this.lastModified = lastModified;
    this.rangeSet = TreeRangeSet.create();
  }

  public String getRemoteFilePath()
  {
    return this.remoteFilePath;
  }

  public long getFileSize()
  {
    return this.fileSize;
  }

  public long getLastModifiedTime()
  {
    return this.lastModified;
  }

  public void addDownloadRange(long startPoint, long endPoint)
  {
    rangeSet.add(Range.open(startPoint, endPoint));
  }

  public RangeSet<Long> getRanges()
  {
    return rangeSet;
  }
}
