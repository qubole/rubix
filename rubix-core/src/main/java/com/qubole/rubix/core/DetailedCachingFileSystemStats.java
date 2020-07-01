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
package com.qubole.rubix.core;

import com.qubole.rubix.spi.BookKeeperFactory;
import org.apache.hadoop.conf.Configuration;
import org.weakref.jmx.Managed;

import static com.qubole.rubix.spi.utils.DataSizeUnits.BYTES;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class DetailedCachingFileSystemStats
    extends BasicCachingFileSystemStats
{
    public DetailedCachingFileSystemStats(
            CachingFileSystemStatsProvider statsProvider,
            BookKeeperFactory bookKeeperFactory,
            Configuration conf)
    {
        super(statsProvider, bookKeeperFactory, conf);
    }

    // RemoteRRC
    @Managed(description = "Get the total MB downloaded for local requests")
    public long getRemote_rrc_data_read()
    {
        return BYTES.toMB(statsProvider.getStats().getRemoteRRCDataRead());
    }

    @Managed(description = "Gets the extra  MB read from remote files due to block alignment for local requests")
    public long getRemote_rrc_extra_data_read()
    {
        return BYTES.toMB(statsProvider.getStats().getRemoteRRCExtraDataRead());
    }

    @Managed(description = "Gets the time in seconds spent in copying data into cache buffers. This is aggregated across all threads hence not the user visible penalty")
    public long getRemote_rrc_warmup_penalty()
    {
        return NANOSECONDS.toSeconds(statsProvider.getStats().getRemoteRRCWarmupTime());
    }

    @Managed(description = "Gets the total number of local requests on cold cache")
    public long getRemote_rrc_requests()
    {
        return statsProvider.getStats().getRemoteRRCRequests();
    }

    // CachedRRC
    @Managed(description = "Gets the MB read from local cache")
    public long getCached_rrc_data_read()
    {
        return BYTES.toMB(statsProvider.getStats().getCachedRRCDataRead());
    }

    @Managed(description = "Gets the total number of request served from local cache")
    public long getCached_rrc_requests()
    {
        return statsProvider.getStats().getCachedRRCRequests();
    }

    // DirectRRC
    @Managed(description = "Gets the total size in MB that was read from Source but not cached")
    public long getDirect_rrc_data_read()
    {
        return BYTES.toMB(statsProvider.getStats().getDirectRRCDataRead());
    }

    @Managed(description = "Gets the number of direct requests")
    public long getDirect_rrc_requests()
    {
        return statsProvider.getStats().getDirectRRCRequests();
    }

    // NonLocalRRC
    @Managed(description = "Gets the total size in MB that was read from non-local caches")
    public long getNonlocal_rrc_data_read()
    {
        return BYTES.toMB(statsProvider.getStats().getNonLocalRRCDataRead());
    }

    @Managed(description = "Gets the total number of non-local reads")
    public long getNonlocal_rrc_requests()
    {
        return statsProvider.getStats().getNonLocalRRCRequests();
    }

    @Managed(description = "Gets the total size in MB that was downloaded for non-local requests")
    public long getBKS_data_downloaded_in_read_through()
    {
        return BYTES.toMB(statsProvider.getStats().getDownloadedFromSourceForNonLocalRequests(bookKeeperFactory, conf));
    }

    @Managed(description = "Gets the extra  MB read from remote files due to block alignment for non-local requests")
    public long getBKS_extra_data_read_in_read_through()
    {
        return BYTES.toMB(statsProvider.getStats().getExtraReadFromSourceForNonLocalRequests(bookKeeperFactory, conf));
    }

    @Managed(description = "Aggregated warmup penalty to non-local reads due to warmup")
    public long getBKS_warmup_penalty_in_read_through()
    {
        return NANOSECONDS.toSeconds(statsProvider.getStats().getWarmupTimeForNonLocalRequests(bookKeeperFactory, conf));
    }

    // Parallel download
    @Managed(description = "Gets the total size in MB that was downloaded by parallel warmup")
    public long getBKS_data_downloaded_in_parallel_warmup()
    {
        return BYTES.toMB(statsProvider.getStats().getDownloadedFromSourceParallel(bookKeeperFactory, conf));
    }

    @Managed(description = "Total walltime of downloads in parallel warmup")
    public long getBKS_time_for_parallel_downloads()
    {
        return NANOSECONDS.toSeconds(statsProvider.getStats().getTotalTimeForParallelDownload(bookKeeperFactory, conf));
    }

    @Managed(description = "Gets the total number corrupted files that have been invalidated")
    public int getcorrupted_file_count()
    {
        return statsProvider.getStats().getCorruptedFileCount();
    }
}
