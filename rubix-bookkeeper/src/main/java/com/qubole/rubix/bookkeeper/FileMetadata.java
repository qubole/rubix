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
import java.nio.channels.FileChannel;
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

    private RandomAccessFile mdFile;
    private FileChannel mdFileChannel;
    private MappedByteBufferBitmap blockBitmap;

    // Striped locks are used to prevent race condition between create and delete of FileMetadata
    // E.g. FileMetadata was being deleted due to SIZE based eviction and another thread access same file leading to
    // a parallel create of FileMetadata for same file. If locking is not done, we might end up with new Metadata reading
    // stale data
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
        int bitmapFileSizeBytes = (int) Math.ceil((double) bitsRequired / 8);

        Lock lock = stripes.get(getRemotePath());
        try {
            lock.lock();
            try {
                mdFile = new RandomAccessFile(mdFilePath, "rw");
            }
            catch (FileNotFoundException e) {
                File file = new File(mdFilePath);
                file.createNewFile();
                file.setWritable(true, false);
                file.setReadable(true, false);
                mdFile = new RandomAccessFile(file, "rw");
            }
            mdFileChannel = mdFile.getChannel();

            blockBitmap = new MappedByteBufferBitmap(mdFileChannel.map(FileChannel.MapMode.READ_WRITE, 0, bitmapFileSizeBytes));
        }
        finally {
            lock.unlock();
        }
    }

    public boolean isBlockCached(long blockNumber)
    {
        return blockBitmap.isSet((int) blockNumber);
    }

    public void setBlockCached(long blockNumber)
    {
        blockBitmap.set((int) blockNumber);
    }

    public void closeAndCleanup(RemovalCause cause, Cache cache)
            throws IOException
    {
        log.info("Evicting " + getRemotePath().toString() + " due to " + cause);
        mdFileChannel.close();
        mdFile.close();
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
    private void deleteFiles(RemovalCause cause, Cache cache)
    {
        Lock lock = stripes.get(getRemotePath());
        try {
            lock.lock();
            if (cache.getIfPresent(getRemotePath()) != null) {
                // It means that another thread raced and created an entry, give up on deletion

                // But if removal was EXPLICIT then we want to delete data in any case e.g. file changed in source
                // Delete data in that case
                if (!cause.equals(RemovalCause.EXPLICIT)) {
                    return;
                }
            }

            File mdFile = new File(mdFilePath);
            mdFile.delete();

            File localFile = new File(localPath);
            localFile.delete();
        }
        finally {
            lock.unlock();
        }
    }

    public int getWeight(Configuration conf)
    {
        // normal entries have no weight
        return 0;
    }
}
