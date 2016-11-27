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
package com.qubole.rubix.core;

import com.qubole.rubix.spi.BookKeeperFactory;
import com.qubole.rubix.spi.CacheConfig;
import com.qubole.rubix.spi.ClusterType;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BufferedFSInputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;

/**
 * Created by sakshia on 25/11/16.
 */
public class TestCachingFileSystem
        extends CachingFileSystem<RawLocalFileSystem>
{
    private static final Log log = LogFactory.getLog(TestCachingFileSystem.class);
    Configuration conf;

    @Override
    public void initialize(URI uri, Configuration conf)
            throws IOException
    {
        this.conf = conf;
        LOG.debug("Initializing TestCachingFileSystem");
        return;
    }

    @Override
    public void setBookKeeper(BookKeeperFactory bookKeeperFactory, Configuration conf)
    {
        this.bookKeeperFactory = bookKeeperFactory;
    }

    @Override
    public URI getUri()
    {
        return null;
    }

    @Override
    public FSDataInputStream open(Path path, int i)
            throws IOException
    {
        String localPath = path.toString().substring(9);
        File file = new File(localPath);
        LocalFSInputStream inputStream = new LocalFSInputStream(localPath);

        return new FSDataInputStream(
                new BufferedFSInputStream(
                        new CachingInputStream(new FSDataInputStream(inputStream), conf, path, file.length(), file.lastModified(),  new CachingFileSystemStats(),
                                ClusterType.TEST_CLUSTER_MANAGER, bookKeeperFactory),
                        CacheConfig.getBlockSize(getConf())));
    }

    @Override
    public FSDataOutputStream create(Path path, FsPermission fsPermission, boolean b, int i, short i1, long l, Progressable progressable)
            throws IOException
    {
        return null;
    }

    @Override
    public FSDataOutputStream append(Path path, int i, Progressable progressable)
            throws IOException
    {
        return null;
    }

    @Override
    public boolean rename(Path path, Path path1)
            throws IOException
    {
        return false;
    }

    @Override
    public boolean delete(Path path)
            throws IOException
    {
        return false;
    }

    @Override
    public boolean delete(Path path, boolean b)
            throws IOException
    {
        return false;
    }

    @Override
    public FileStatus[] listStatus(Path path)
            throws FileNotFoundException, IOException
    {
        return new FileStatus[0];
    }

    @Override
    public void setWorkingDirectory(Path path)
    {

    }

    @Override
    public Path getWorkingDirectory()
    {
        return null;
    }

    @Override
    public boolean mkdirs(Path path, FsPermission fsPermission)
            throws IOException
    {
        return false;
    }

    @Override
    public FileStatus getFileStatus(Path path)
            throws IOException
    {
        return null;
    }
}
