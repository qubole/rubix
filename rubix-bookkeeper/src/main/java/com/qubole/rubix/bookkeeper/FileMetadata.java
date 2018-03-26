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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.RemovalCause;
import com.google.common.util.concurrent.Striped;
import com.qubole.rubix.spi.CacheConfig;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.locks.Lock;

import static com.qubole.rubix.spi.CacheConfig.getBlockSize;

/**
 * Created by stagra on 29/12/15.
 */
public class FileMetadata
{
  private String remotePath;
  private String localPath;
  private String mdFilePath;
  private long size;
  private long lastModified;

  private boolean needsRefresh = true;

  int bitmapFileSizeBytes;
  ByteBufferBitmap blockBitmap;

  static Striped<Lock> stripes = Striped.lock(20000);

  private static Log log = LogFactory.getLog(FileMetadata.class.getName());

  public FileMetadata()
  {
  }

  public FileMetadata(String remotePath, long fileLength, long lastModified, Configuration conf)
      throws IOException
  {
    this.remotePath = remotePath;
    this.size = fileLength;
    this.lastModified = lastModified;
    localPath = CacheConfig.getLocalPath(remotePath, conf);
    mdFilePath = CacheConfig.getMDFile(remotePath, conf);

    int bitsRequired = (int) Math.ceil((double) size / getBlockSize(conf)); //numBlocks
    bitmapFileSizeBytes = (int) Math.ceil((double) bitsRequired / 8);

    /*
     * Caution: Do no call refreshBitmap in constructor as it breaks the assumptions in delete path and it could
     * cause race conditions
     */
  }

  public void setNeedsRefresh()
  {
    needsRefresh = true;
  }

  public void refreshBitmap()
      throws IOException
  {
    byte[] bytes = new byte[bitmapFileSizeBytes];
    RandomAccessFile mdFile;
    Lock lock = stripes.get(remotePath);
    try {
      lock.lock();
      try {
        mdFile = new RandomAccessFile(mdFilePath, "rw");
        mdFile.readFully(bytes, 0, (int) mdFile.length());
      }
      catch (FileNotFoundException e) {
        File file = new File(mdFilePath);
        file.createNewFile();
        file.setWritable(true, false);
        file.setReadable(true, false);
        mdFile = new RandomAccessFile(file, "rw");
        mdFile.setLength(bitmapFileSizeBytes);
      }
      mdFile.close();
    }
    finally {
      lock.unlock();
    }
    blockBitmap = new ByteBufferBitmap(bytes);
    needsRefresh = false;
  }

  public boolean isBlockCached(long blockNumber)
      throws IOException
  {
    if (needsRefresh) {
      refreshBitmap();
    }
    return blockBitmap.isSet((int) blockNumber);
  }

  private void setBlockCached(long blockNumber)
      throws IOException
  {
    if (needsRefresh) {
      refreshBitmap();
    }

    blockBitmap.set((int) blockNumber);
  }

  // Returns true if backing mdfile is updated
  public synchronized boolean setBlocksCached(long startBlock, long endBlock)
      throws IOException
  {
    for (long blockNum = startBlock; blockNum < endBlock; blockNum++) {
      setBlockCached(blockNum);
    }

    // update mdfile
    try {
      RandomAccessFile mdFile = new RandomAccessFile(mdFilePath, "rw");
      mdFile.write(blockBitmap.getBytes());
      mdFile.close();
    }
    catch (FileNotFoundException e) {
      // it is possible that file is deleted by an old CacheEviction event after this FileMetadata entry was made. See 3.1.2 comment above
      // refresh
      log.error("Could not update mdfile for " + remotePath + ". Trying again", e);
      try {
        refreshBitmap();
      }
      catch (IOException e1) {
        // Inconsistent state, reset bitmap to prevent unknown issues
        blockBitmap = new ByteBufferBitmap(new byte[bitmapFileSizeBytes]);
        log.error("Could not refresh mdfile in second try for " + remotePath, e);
      }
      log.warn("Updated mdfile successfully for " + remotePath);
    }
    catch (IOException e) {
      log.error("Could not update mdfile for " + remotePath, e);
      return false;
    }

    return true;
  }

  public void closeAndCleanup(RemovalCause cause, Cache cache)
      throws IOException
  {
    log.warn("Evicting " + getRemotePath().toString() + " due to " + cause);
    deleteFiles(cause, cache);
  }

  public long getLastModified()
  {
    return lastModified;
  }

  @VisibleForTesting
  public String getMdFilePath()
  {
    return mdFilePath;
  }

  public String getRemotePath()
  {
    return remotePath;
  }

  // Assumption: this is called after the FileMetadata has been removed from cache
  // E.g. in RemovalListener
  private void deleteFiles(RemovalCause cause, Cache<String, FileMetadata> cache)
  {
    /*
     * Cases of races when thread T1 trying to add new entry to cache and T2 is removing deleting data for same
     * 1. T2 DeleteFiles |in parallel to| T1 Create FileMetada : Safe, T1 will create new mdfile and start from blank state
     * 2. T2 DeleteFiles |in parallel to| T1 added entry to cache: Safe, T1 still did not load mdfile.
     * 3. T2 DeleteFiles |in parallel to| T1 refreshBitmap:
     *          3.1. T2 gets lock first -> deletes data -> T1 refreshes (maybe twice as T2 sets needsRfresh): => Safe
     *          3.2. T1 refreshes -> T2 deletes -> T2 sets needsRefresh => next T1 operation refreshes
     *                          One operation on T1 would fail in read in RRC but CachingInputStream will handle failure
     */
    Lock lock = stripes.get(getRemotePath());
    try {
      lock.lock();

      File mdFile = new File(mdFilePath);
      mdFile.delete();

      File localFile = new File(localPath);
      localFile.delete();
    }
    finally {
      lock.unlock();
    }

    FileMetadata newEntry = cache.getIfPresent(getRemotePath());
    if (newEntry != null) {
      newEntry.setNeedsRefresh();
    }
  }

  public int getWeight(Configuration conf)
  {
    // normal entries have no weight
    return 1;
  }
}
