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

package com.qubole.rubix.common;

/**
 * Created by Abhishek on 2/15/18.
 */
public final class MetricsConstant
{
  private MetricsConstant()
  {
  }

  public static final String CACHED_READ_BYTES = "cachedReadBytes";
  public static final String REMOTE_READ_BYTES = "remoteReadBytes";
  public static final String NONLOCAL_READ_BYTES = "nonlocalReadBytes";

  public static final String CACHED_READ_REQUESTS = "cachedReadRequests";
  public static final String REMOTE_READ_REQUESTS = "remoteReadRequests";
  public static final String NONLOCAL_READ_REQUESTS = "nonlocalReadRequests";

  public static final String CACHE_DISK_SPACE_USED = "cacheDiskSpaceUsed";

  public static final String NUM_FILES_EVICTED = "numFilesEvicted";
  public static final String NUM_FILES_EVICTED_EXPLICIT = "numFilesEvictedExplicit";
  public static final String NUM_FILES_EVICTED_SIZE = "numFilesEvictedSize";
}
