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

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import org.apache.hadoop.conf.Configuration;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/**
 * Created by stagra on 14/2/16.
 */
public class BookKeeperConfig
{
    public static String DATA_CACHE_EXPIRATION = "hadoop.cache.data.expiration";
    public static String DATA_CACHE_EXIPIRATION_AFTER_WRITE = "hadoop.cache.data.expiration.after-write";
    public static String DATA_CACHE_FULLNESS = "hadoop.cache.data.fullness.percentage";
    public static String DATA_CACHE_DIRPREFIXES = "hadoop.cache.data.dirprefix.list";
    public static String BLOCK_SIZE = "hadoop.cache.data.block-size";
    public static String DATA_CACHE_BOOKKEEPER_PORT = "hadoop.cache.data.bookkeeper.port";
    public static String DATA_CACHE_BOOKKEEPER_MAX_THREADS = "hadoop.cache.data.bookkeeper.max-threads";

    static String FILE_CACHE_DIR_SUFFIX = "/fcache/";
    static int MAX_DISKS = 5;

    // default values
    private static final int dataCacheExpiry = Integer.MAX_VALUE;
    // Keepnig this low to workaround the Guava Cache static weighing limitation
    private static final int dataCacheExpiryAfterWrite = 300; //In sec
    private static final int dataCacheFullness = 80; // percent
    private static final String dataCacheDirPrefixes = "/media/ephemeral";
    private static final int blockSize = 1 * 1024 * 1024; // 1MB
    private static int serverPort = 8899;
    private static int serverMaxThreads = Integer.MAX_VALUE;

    public static int getCacheDataExpiration(Configuration conf)
    {
        return conf.getInt(DATA_CACHE_EXPIRATION, dataCacheExpiry);
    }

    public static int getCacheDataExpirationAfterWrite(Configuration conf)
    {
        return conf.getInt(DATA_CACHE_EXIPIRATION_AFTER_WRITE, dataCacheExpiryAfterWrite);
    }

    public static int getCacheDataFullnessPercentage(Configuration conf)
    {
        return conf.getInt(DATA_CACHE_FULLNESS, dataCacheFullness);
    }

    public static String getCacheDirPrefixList(Configuration conf)
    {
        return conf.get(DATA_CACHE_DIRPREFIXES, dataCacheDirPrefixes);
    }

    public static int getBlockSize(Configuration conf)
    {
        return conf.getInt(BLOCK_SIZE, blockSize);
    }

    public static int getServerPort(Configuration conf)
    {
        return conf.getInt(DATA_CACHE_BOOKKEEPER_PORT, serverPort);
    }

    public static int getServerMaxThreads(Configuration conf)
    {
        return conf.getInt(DATA_CACHE_BOOKKEEPER_MAX_THREADS, serverMaxThreads);
    }

    public static int numDisks(Configuration conf)
    {
        return getDiskPathsMap(conf).size();
    }

    public static HashMap<Integer, String> getDiskPathsMap(final Configuration conf)
    {
        Supplier<HashMap<Integer, String>> s = Suppliers.memoize(new Supplier<HashMap<Integer, String>>()
        {
            @Override
            public HashMap<Integer, String> get()
            {
                HashMap<Integer, String> dirPathMap = new HashMap<>();
                int ndisks = 0;
                List<String> dirPrefixList = getDirPrefixList(conf);
                for (String dirPrefix : dirPrefixList) {
                    for (int i = 0; i < MAX_DISKS; ++i) {
                        if (exists(dirPrefix + i)) {
                            File dir = new File(dirPrefix + i + FILE_CACHE_DIR_SUFFIX);
                            dir.mkdir();
                            dirPathMap.put(ndisks, dirPrefix + i);
                            ++ndisks;
                        }
                    }
                }
                return dirPathMap;
            }
        });
        return s.get();
    }

    private static List<String> getDirPrefixList(Configuration conf)
    {
        String cacheDirPrefixList = getCacheDirPrefixList(conf);
        return Arrays.asList(cacheDirPrefixList.split("\\s*,\\s*"));
    }

    public static String getDirPath(Configuration conf, int d)
    {
        HashMap<Integer, String> dirPrefixMap =  getDiskPathsMap(conf);
        return dirPrefixMap.get(d);
    }

    private static boolean exists(String filename)
    {
        return (new File(filename)).exists();
    }

    public static String getLocalPath(String remotePath, Configuration conf)
    {
        String absLocation = getDirectory(remotePath, conf);
        return absLocation + "/" + getName(remotePath);
    }

    public static String getMDFile(String remotePath, Configuration conf)
    {
        String absLocation = getDirectory(remotePath, conf);
        return absLocation + "/" + getName(remotePath) + "_mdfile";
    }

    public static String getDirectory(String remotePath, Configuration conf)
    {
        String parentPath = getParent(remotePath);
        String relLocation = parentPath.indexOf(':') == -1 ? parentPath :parentPath.substring(parentPath.indexOf(':') + 3);
        String absLocation = getLocalDirFor(remotePath, conf) + relLocation;
        File parent = new File(absLocation);
        parent.mkdirs();
        return absLocation;
    }

    public static String getLocalDirFor(String remotePath, Configuration conf)
    {
        int h = Math.abs(remotePath.hashCode());
        int d = h % numDisks(conf);
        String dirname = getDirPath(conf, d) + BookKeeperConfig.FILE_CACHE_DIR_SUFFIX ;
        return dirname;
    }

    public static String getName(String remotePath)
    {
        return remotePath.lastIndexOf('/') == -1 ? remotePath : remotePath.substring(remotePath.lastIndexOf('/'));
    }

    public static String getParent(String remotePath)
    {
        return remotePath.lastIndexOf('/') == -1 ? "" : remotePath.substring(0, remotePath.lastIndexOf('/'));
    }
}
