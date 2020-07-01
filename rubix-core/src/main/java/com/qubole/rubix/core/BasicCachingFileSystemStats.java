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

public class BasicCachingFileSystemStats
{
    protected final CachingFileSystemStatsProvider statsProvider;
    protected final BookKeeperFactory bookKeeperFactory;
    protected final Configuration conf;

    public BasicCachingFileSystemStats(
            CachingFileSystemStatsProvider statsProvider,
            BookKeeperFactory bookKeeperFactory,
            Configuration conf)
    {
        this.statsProvider = statsProvider;
        this.bookKeeperFactory = bookKeeperFactory;
        this.conf = conf;
    }

    @Managed(description = "Gets the total size in MB that was read from Source")
    public long getMB_read_from_source()
    {
        return BYTES.toMB(statsProvider.getStats().getDirectRRCDataRead() +
                statsProvider.getStats().getRemoteRRCDataRead() +
                statsProvider.getStats().getDownloadedFromSourceForNonLocalRequests(bookKeeperFactory, conf));
    }

    @Managed(description = "Gets the total size in MB that was read from cache")
    public long getMB_read_from_cache()
    {
        // Gives accurate value at node level when parallel warmup is enabled
        // But when parallel warmup is disabled then even the non local reads that caused warmup on the other node
        // are accounted in cached reads. For accurate accounting at cluster level in that case:
        // readFromCache = sum(MBReadFromCache from each node) - sum(MBDownloadedFromSourceForNonLocalRequests from each node)
        // Note that we cannot do (getCachedRRCDataRead + getNonLocalRRCDataRead - getDownloadedFromSourceForNonLocalRequests)
        // because getDownloadedFromSourceForNonLocalRequests would be for requests of other nodes and at node level this
        // formula will not give the right value of data read from cache
        return BYTES.toMB(
                statsProvider.getStats().getCachedRRCDataRead() +
                        statsProvider.getStats().getNonLocalRRCDataRead());
    }

    @Managed(description = "Return cache hit ratio")
    public double getCache_hit()
    {
        long readFromCache = getMB_read_from_cache();
        return (double) readFromCache / (readFromCache + getMB_read_from_source());
    }
}
