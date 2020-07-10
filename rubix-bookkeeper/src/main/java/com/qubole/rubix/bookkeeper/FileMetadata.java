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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.RemovalCause;
import com.google.common.hash.BloomFilter;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.Striped;
import com.qubole.rubix.spi.CacheConfig;
import com.qubole.rubix.spi.CacheUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.OptionalInt;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.Lock;

import static com.qubole.rubix.spi.utils.DataSizeUnits.BYTES;
import static com.qubole.rubix.spi.CacheConfig.getBlockSize;
import static com.qubole.rubix.spi.CacheUtil.DUMMY_MODE_GENERATION_NUMBER;
import static com.qubole.rubix.spi.CacheUtil.UNKONWN_GENERATION_NUMBER;

/**
 * Created by stagra on 29/12/15.
 */
public class FileMetadata
{
  private final String remotePath;
  private final String localPath;
  private final String mdFilePath;
  private final long size;
  private final long lastModified;
  private long currentFileSize;
  private boolean needsRefresh = true;
  private final int generationNumber;

  int bitmapFileSizeBytes;
  ByteBufferBitmap blockBitmap;

  private static final Striped<Lock> stripes = Striped.lock(20000);

  private static final Log log = LogFactory.getLog(FileMetadata.class.getName());

  // This constructor should not be called in parallel for same remotePath
  public FileMetadata(String remotePath,
      long fileLength,
      long lastModified,
      long currentFileSize,
      Configuration conf,
      Cache<String, Integer> generationNumberCache,
      BloomFilter fileAccessedBloomFilter)
      throws ExecutionException, IOException
  {
    this(remotePath,
        fileLength,
        lastModified,
        currentFileSize,
        conf,
        findGenerationNumber(remotePath, conf, generationNumberCache, fileAccessedBloomFilter));
    createLocalFiles();
  }


  public FileMetadata(String remotePath,
      long fileLength,
      long lastModified,
      long currentFileSize,
      Configuration conf,
      int generationNumber)
  {
    this.remotePath = remotePath;
    this.size = fileLength;
    this.lastModified = lastModified;
    this.currentFileSize = currentFileSize;
    this.generationNumber = generationNumber;
    localPath = CacheUtil.getLocalPath(remotePath, conf, generationNumber);
    mdFilePath = CacheUtil.getMetadataFilePath(remotePath, conf, generationNumber);
    int bitsRequired = (int) Math.ceil((double) size / getBlockSize(conf)); //numBlocks
    bitmapFileSizeBytes = (int) Math.ceil((double) bitsRequired / 8);

    /*
     * Caution: Do no call refreshBitmap in constructor as it breaks the assumptions in delete path and it could
     * cause race conditions
     */
  }

  /**
   * BKS is responsible for creating new files (data + mdfiles) via FileMetadata in all cases.
   * RRCs should never create new files, if they ever hit a case of FNF then
   * they should fail the request as it points to invalidations.
   */
  private void createLocalFiles()
      throws IOException
  {
    log.debug(String.format("Creating Local Files %s and %s ", localPath, mdFilePath));
    File file = new File(localPath);
    file.createNewFile();
    file.setWritable(true, false);
    file.setReadable(true, false);
    file = new File(mdFilePath);
    file.createNewFile();
    file.setWritable(true, false);
    file.setReadable(true, false);
  }

  // Should not be called in parallel for the same remotePath
  private static int findGenerationNumber(String remotePath,
      Configuration conf,
      Cache<String, Integer> generationNumberCache,
      BloomFilter fileAccessedBloomFilter)
      throws ExecutionException
  {
    // For Dummy-Mode, stay at fixed generationNumber to avoid complications of fetching generation number
    // in updateCacheStatus calls of NonLocalReads
    if (CacheConfig.isDummyModeEnabled(conf)) {
      return DUMMY_MODE_GENERATION_NUMBER;
    }

    int genNumber;
    Closer oldFilesRemover = Closer.create();

    if (!fileAccessedBloomFilter.mightContain(remotePath)) {
      // first access to the file since BKS started

      // Find the highest genNumber based on files on disk
      int highestGenNumberOnDisk = UNKONWN_GENERATION_NUMBER + 1;
      while (new File(CacheUtil.getLocalPath(remotePath, conf, highestGenNumberOnDisk)).exists() ||
              new File(CacheUtil.getMetadataFilePath(remotePath, conf, highestGenNumberOnDisk)).exists()) {
        highestGenNumberOnDisk++;
      }
      highestGenNumberOnDisk--;
      if (CacheConfig.isCleanupFilesDuringStartEnabled(conf)) {
        // Pick the generationNumber as one more than the highestGenNumberOnDisk
        addFilesForDeletion(oldFilesRemover, highestGenNumberOnDisk, remotePath, conf);
        genNumber = highestGenNumberOnDisk + 1;
      }
      else {
        // If no files exists for this path on disk then start with genNum = 1
        if (highestGenNumberOnDisk == UNKONWN_GENERATION_NUMBER) {
          genNumber = 1;
        }
        // If both datafile and mdfile exist for highestGenNumberOnDisk, use that as genNumber
        else if (new File(CacheUtil.getLocalPath(remotePath, conf, highestGenNumberOnDisk)).exists() &&
                new File(CacheUtil.getMetadataFilePath(remotePath, conf, highestGenNumberOnDisk)).exists()) {
          addFilesForDeletion(oldFilesRemover, highestGenNumberOnDisk - 1, remotePath, conf);
          genNumber = highestGenNumberOnDisk;
        }
        else {
          addFilesForDeletion(oldFilesRemover, highestGenNumberOnDisk, remotePath, conf);
          genNumber = highestGenNumberOnDisk + 1;
        }
      }
      fileAccessedBloomFilter.put(remotePath);
    }
    else {
      genNumber = generationNumberCache.get(remotePath, () -> UNKONWN_GENERATION_NUMBER) + 1;
      while (new File(CacheUtil.getLocalPath(remotePath, conf, genNumber)).exists() ||
              new File(CacheUtil.getMetadataFilePath(remotePath, conf, genNumber)).exists()) {
        genNumber++;
      }
      addFilesForDeletion(oldFilesRemover, genNumber - 1, remotePath, conf);
    }
    generationNumberCache.put(remotePath, genNumber);
    try {
      oldFilesRemover.close();
    }
    catch (IOException e) {
      log.warn("Exception while deleting old files", e);
    }
    return genNumber;
  }

  private static void addFilesForDeletion(Closer fileRemover, int generationNumber, String remotePath, Configuration conf)
  {
    for (int i = 1; i <= generationNumber; i++) {
      fileRemover.register(new File(CacheUtil.getLocalPath(remotePath, conf, i))::delete);
      fileRemover.register(new File(CacheUtil.getMetadataFilePath(remotePath, conf, i))::delete);
    }
  }

  long incrementCurrentFileSize(long incrementBy)
  {
    this.currentFileSize += incrementBy;
    return this.currentFileSize;
  }

  @VisibleForTesting
  public long getCurrentFileSize()
  {
    return currentFileSize;
  }

  public void setNeedsRefresh()
  {
    needsRefresh = true;
  }

  void refreshBitmap()
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

  /*
   * Returns number of blocks marked cached that were not in cache already,
   * empty in case errors
   */
  public synchronized OptionalInt setBlocksCached(long startBlock, long endBlock)
      throws IOException
  {
    int numberOfBlocksUpdated = 0;
    for (long blockNum = startBlock; blockNum < endBlock; blockNum++) {
      if (!isBlockCached(blockNum)) {
        numberOfBlocksUpdated++;
        setBlockCached(blockNum);
      }
    }
    // update mdfile
    try {
      RandomAccessFile mdFile = new RandomAccessFile(mdFilePath, "rw");
      mdFile.write(blockBitmap.getBytes());
      mdFile.close();
    }
    catch (FileNotFoundException e) {
      numberOfBlocksUpdated = -1;

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
      numberOfBlocksUpdated = -1;
    }

    return numberOfBlocksUpdated == -1 ? OptionalInt.empty() : OptionalInt.of(numberOfBlocksUpdated);
  }

  public void closeAndCleanup(RemovalCause cause, Cache cache)
  {
    if (cause != RemovalCause.REPLACED) {
      log.warn("Evicting " + getRemotePath() + " due to " + cause);
      deleteFiles(cache);
    }
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

  public long getFileSize()
  {
    return this.size;
  }

  // Assumption: this is called after the FileMetadata has been removed from cache
  // E.g. in RemovalListener
  void deleteFiles(Cache<String, FileMetadata> cache)
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
      if (!mdFile.delete()) {
        log.error("Failed to delete metadata file " + mdFilePath);
      }

      File localFile = new File(localPath);
      if (!localFile.delete()) {
        log.error("Failed to delete local file " + localPath);
      }
    }
    finally {
      lock.unlock();
    }

    FileMetadata newEntry = cache.getIfPresent(getRemotePath());
    if (newEntry != null) {
      newEntry.setNeedsRefresh();
    }
  }

  // Returns the current downloaded fileSize in KB
  public int getWeight()
  {
    return Math.toIntExact(BYTES.toKB(currentFileSize));
  }

  public int getGenerationNumber()
  {
    return generationNumber;
  }
}
