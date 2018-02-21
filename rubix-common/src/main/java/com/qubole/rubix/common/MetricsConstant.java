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

  public static final String CACHED_READ_BYTES = "cached_read_bytes";
  public static final String REMOTE_READ_BYTES = "remote_read_bytes";
  public static final String NONLOCAL_READ_BYTES = "nonlocal_read_bytes";

  public static final String CACHED_READ_REQUESTS = "cached_read_requests";
  public static final String REMOTE_READ_REQUESTS = "remote_read_requests";
  public static final String NONLOCAL_READ_REQUESTS = "nonlocal_read_requests";

  public static final String CACHE_DISK_SPACE_USED = "cache_disk_space_used";

  public static final String NUM_FILES_EVICTED = "num_files_evicted";
  public static final String NUM_FILES_EVICTED_EXPLICIT = "num_files_evicted_explicit";
  public static final String NUM_FILES_EVICTED_SIZE = "num_files_evicted_size";
}
