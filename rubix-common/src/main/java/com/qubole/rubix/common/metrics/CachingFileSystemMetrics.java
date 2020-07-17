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
package com.qubole.rubix.common.metrics;

import java.util.HashMap;
import java.util.Map;

public enum CachingFileSystemMetrics {
    LOCAL_FALLBACK_TO_DIRECT_READ("rubix_local_cache_fallback_direct_read"),
    NON_LOCAL_FALLBACK_TO_DIRECT_READ("rubix_non_local_cache_fallback_direct_read"),
    POSITIONAL_READ_FAILURE("rubix_positional_read_failure");

    private final String metricName;
    //reverse lookup map for metric.
    private static final Map<String, CachingFileSystemMetrics> lookup = new HashMap<>();

    CachingFileSystemMetrics(String metricName)
    {
        this.metricName = metricName;
    }

    public String getMetricName()
    {
        return metricName;
    }

    static {
        for(CachingFileSystemMetrics s : CachingFileSystemMetrics.values())
            lookup.put(s.getMetricName(), s);
    }

    public static CachingFileSystemMetrics get(String enumString) {
        return lookup.get(enumString);
    }
}
