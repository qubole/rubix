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

import com.google.common.base.Throwables;
import com.google.common.cache.Cache;
import com.google.common.cache.RemovalCause;
import com.google.common.util.concurrent.AbstractScheduledService;
import com.qubole.rubix.spi.CacheConfig;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.qubole.rubix.bookkeeper.utils.DiskUtils.getUsedSpaceMB;

/**
 * Created by shubham on 19/05/17.
 */
public class DiskMonitorService extends AbstractScheduledService
{
  private Executor executor = Executors.newSingleThreadExecutor();
  private final int interval;
  private final BookKeeper bookKeeper;

  private static Log log = LogFactory.getLog(DiskMonitorService.class.getName());

  private static final Callable METADATA_LOADER = new CreateDiskUsageFileMetadataCallable();
  private static final FileMetadata DISK_USAGE_ENTRY = new DiskUsageFileMetada();
  private static final String DISK_USAGE_KEY = "DISK_USAGE_KEY";

  @Override
  protected void runOneIteration() throws Exception
  {
    try {
      bookKeeper.invalidateEntry(DISK_USAGE_KEY);
      bookKeeper.getEntry(DISK_USAGE_KEY, METADATA_LOADER);
    }
    catch (ExecutionException e) {
      log.error("Could not refresh disk usage", e);
    }
  }

  public DiskMonitorService(Configuration configuration, BookKeeper bookKeeper)
  {
    interval = CacheConfig.getDiskMonitorInterval(configuration);
    this.bookKeeper = bookKeeper;
  }

  @Override
  protected Scheduler scheduler()
  {
    return Scheduler.newFixedDelaySchedule(60, interval, TimeUnit.SECONDS);
  }

  static class FailureListener
      extends com.google.common.util.concurrent.Service.Listener
  {
    @Override
    public void failed(State from, Throwable failure)
    {
      super.failed(from, failure);
      log.error("hit a problem " + Throwables.getStackTraceAsString(failure));
    }
  }

  @Override
  protected void startUp()
  {
    log.info("starting service " + serviceName() + " in thread " + Thread.currentThread().getId());
    addListener(new FailureListener(), executor);
  }

  private static class DiskUsageFileMetada
      extends FileMetadata
  {
    @Override
    public void closeAndCleanup(RemovalCause cause, Cache cache)
        throws IOException
    {
      log.info("Removing  disk_usage_entry due to " + cause);
      return;
    }

    @Override
    public int getWeight(Configuration conf)
    {
      /*
       * Following new logic for evictions:
       *  All FileMetada entries have weight = 0
       *  There is a special entry DISK_USAGE_ENTRY which maps to the amount of space used on disk
       *  This entry is updated periodically by a daemon
       *  Evictions due to disk space exhaustion are driven by this entry
       *  Since guava cache evicts LRU based and not weighted, actual entries will get removed
       */
      int weight = getUsedSpaceMB(conf);
      log.info("UsedSpace " + weight);
      return weight;
    }

    // Methods below should not be called on this class
    @Override
    public boolean isBlockCached(long blockNumber)
    {
      throw new UnsupportedOperationException("Got a isBLockCached request for Disk_Usage FileMetadata entry ");
    }

    @Override
    public boolean setBlocksCached(long start, long end)
    {
      throw new UnsupportedOperationException("Got a setBLockCached request for Disk_Usage FileMetadata entry ");
    }

    @Override
    public long getLastModified()
    {
      throw new UnsupportedOperationException("Got a getLastModified request for Disk_Usage FileMetadata entry ");
    }

    @Override
    public String getMdFilePath()
    {
      throw new UnsupportedOperationException("Got a getMdFilePath request for Disk_Usage FileMetadata entry ");
    }

    @Override
    public String getRemotePath()
    {
      throw new UnsupportedOperationException("Got a getRemotePath request for Disk_Usage FileMetadata entry ");
    }
  }

  private static class CreateDiskUsageFileMetadataCallable
      implements Callable<FileMetadata>
  {
    @Override
    public FileMetadata call() throws Exception
    {
      return DISK_USAGE_ENTRY;
    }
  }
}
