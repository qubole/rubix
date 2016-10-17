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
import com.qubole.rubix.spi.CacheConfig;
import org.apache.hadoop.conf.Configuration;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

import static com.qubole.rubix.bookkeeper.utils.DiskUtils.getActualSize;
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

    public FileMetadata(String remotePath, long fileLength, long lastModified, Configuration conf)
            throws IOException
    {
        this.remotePath = remotePath;
        this.size = fileLength;
        this.lastModified = lastModified;
        localPath = CacheConfig.getLocalPath(remotePath, conf);
        mdFilePath = CacheConfig.getMDFile(remotePath, conf);

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

        int bitsRequired = (int) Math.ceil((double) size / getBlockSize(conf)); //numBlocks
        int bitmapFileSizeBytes = (int) Math.ceil((double) bitsRequired / 8);

        blockBitmap = new MappedByteBufferBitmap(mdFileChannel.map(FileChannel.MapMode.READ_WRITE, 0, bitmapFileSizeBytes));
    }

    public boolean isBlockCached(long blockNumber)
    {
        return blockBitmap.isSet((int) blockNumber);
    }

    public void setBlockCached(long blockNumber)
    {
        blockBitmap.set((int) blockNumber);
    }

    public void close()
            throws IOException
    {
        mdFileChannel.close();
        mdFile.close();
    }

    public void closeAndCleanup()
            throws IOException
    {
        close();
        deleteFiles();
    }

    public String getLocalPath()
    {
        return localPath;
    }

    public long getFileSize()
    {
        return size;
    }

    public long getLastModified()
    {
        return lastModified;
    }

    /*
     * This method returns the actual occupied size of data file on disk
     * If file is not present yet, it will return the full file size
     */
    public long getOccupiedSize()
    {
        long size;
        try {
            size = getActualSize(localPath);
        }
        catch (IOException | NumberFormatException e) {
            // if any error then use the remote file's size
            // error can happen when it is the first time and file does not exist locally
            // or some issue in getting actual size
            size = this.size;
        }

        return size;
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

    private void deleteFiles()
    {
        File mdFile = new File(mdFilePath);
        mdFile.delete();

        File localFile = new File(localPath);
        localFile.delete();
    }
}
