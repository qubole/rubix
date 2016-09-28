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
package com.qubole.rubix.spi;

import com.google.common.base.Charsets;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.apache.hadoop.conf.Configuration;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/**
 * Created by stagra on 14/2/16.
 */
public class CacheConfig
{
    public static String dataCacheExpirationConf = "hadoop.cache.data.expiration";
    public static String dataCacheExpirationAfterWriteConf = "hadoop.cache.data.expiration.after-write";
    public static String dataCacheFullnessConf = "hadoop.cache.data.fullness.percentage";
    public static String dataCacheDirprefixesConf = "hadoop.cache.data.dirprefix.list";
    public static String blockSizeConf = "hadoop.cache.data.block-size";
    public static String dataCacheBookkeeperPortConf = "hadoop.cache.data.bookkeeper.port";
    public static String dataCacheBookkeeperMaxThreadsConf = "hadoop.cache.data.bookkeeper.max-threads";

    static String fileCacheDirSuffixConf = "/fcache/";
    static int maxDisksConf = 5;

    // default values
    private static final int dataCacheExpiry = Integer.MAX_VALUE;
    // Keepnig this low to workaround the Guava Cache static weighing limitation
    private static final int dataCacheExpiryAfterWrite = 300; //In sec
    private static final int dataCacheFullness = 80; // percent
    private static final String dataCacheDirPrefixes = "/media/ephemeral";
    private static final int blockSize = 1 * 1024 * 1024; // 1MB
    private static int serverPort = 8899;
    private static int serverMaxThreads = Integer.MAX_VALUE;

    private CacheConfig()
    {
    }

    public static int getCacheDataExpiration(Configuration conf)
    {
        return conf.getInt(dataCacheExpirationConf, dataCacheExpiry);
    }

    public static int getCacheDataExpirationAfterWrite(Configuration conf)
    {
        return conf.getInt(dataCacheExpirationAfterWriteConf, dataCacheExpiryAfterWrite);
    }

    public static int getCacheDataFullnessPercentage(Configuration conf)
    {
        return conf.getInt(dataCacheFullnessConf, dataCacheFullness);
    }

    public static String getCacheDirPrefixList(Configuration conf)
    {
        return conf.get(dataCacheDirprefixesConf, dataCacheDirPrefixes);
    }

    public static int getBlockSize(Configuration conf)
    {
        return conf.getInt(blockSizeConf, blockSize);
    }

    public static int getServerPort(Configuration conf)
    {
        return conf.getInt(dataCacheBookkeeperPortConf, serverPort);
    }

    public static int getServerMaxThreads(Configuration conf)
    {
        return conf.getInt(dataCacheBookkeeperMaxThreadsConf, serverMaxThreads);
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
                    for (int i = 0; i < maxDisksConf; ++i) {
                        if (exists(dirPrefix + i)) {
                            File dir = new File(dirPrefix + i + fileCacheDirSuffixConf);
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
        HashMap<Integer, String> dirPrefixMap = getDiskPathsMap(conf);
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
        String relLocation = parentPath.indexOf(':') == -1 ? parentPath : parentPath.substring(parentPath.indexOf(':') + 3);
        String absLocation = getLocalDirFor(remotePath, conf) + relLocation;
        File parent = new File(absLocation);
        parent.mkdirs();
        return absLocation;
    }

    public static String getLocalDirFor(String remotePath, Configuration conf)
    {
        int numDisks = numDisks(conf);
        int numBuckets = 100 * numDisks;
        HashFunction hf = Hashing.murmur3_32();
        HashCode hc = hf.hashString(remotePath, Charsets.UTF_8);
        int bucket = Math.abs(hc.asInt()) % numBuckets;
        int dirNum = (bucket / numDisks) % numDisks;

        String dirname = getDirPath(conf, dirNum) + CacheConfig.fileCacheDirSuffixConf;
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
