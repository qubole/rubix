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

import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.util.Set;

/**
 * Created by stagra on 25/1/16.
 */

/*
 * The conf provided the CachingFileSystem should have appropriate configs set if user wish to override defaults
 * Different engines provides different ways to do this:
 * Presto: "hive.config.resources" can point to files storing hadoop configurations
 * Hive: --hiveconf argument can be used
 * Spark: --config spark.hadoop.XYZ can be used
 */
public class CachingConfigHelper
{
    public static final String DATA_CACHE_ENABLED = "hadoop.cache.data.enabled";
    public static final String DATA_CACHE_TABLE_WHITELIST = "hadoop.cache.data.table.whitelist";
    public static final String DATA_CACHE_TABLE = "hadoop.cache.data.table";
    public static final String DATA_CACHE_LOCATION_BLACKLIST = "hadoop.cache.data.location.blacklist"; // these locations will be skipped
    public static final String DATA_CACHE_TABLE_MIN_COLS = "hadoop.cache.data.table.columns.min";
    public static final String DATA_CACHE_TABLE_COLS_CHOSEN = "hadoop.cache.data.table.columns.chosen";

    // Internal
    public static final String LOCALITY_INFO_FORWARDED = "hadoop.cache.locality.info.forwarded";
    public static final String LOCALITY_INFO = "hadoop.cache.locality.info";

    // In strict mode, queries will error out if BookKeeper cannot be reached
    public static final String DATA_CACHE_STRICT_MODE = "hadoop.cache.data.strict.mode";

    private CachingConfigHelper()
    {
        // not allowed
    }

    static boolean isCacheDataEnabled(Configuration c)
    {
        return c.getBoolean(DATA_CACHE_ENABLED, true);
    }

    // Configs below need support from engines and wouldn't help unless corresponding setters are called from inside engines

    static String getCacheDataTableWhitelist(Configuration c)
    {
        return c.get(DATA_CACHE_TABLE_WHITELIST, ".*");
    }

    static void setCacheDataTable(Configuration configuration, String table)
    {
        configuration.set(DATA_CACHE_TABLE, table);
    }

    static String getCacheDataTable(Configuration configuration)
    {
        return configuration.get(DATA_CACHE_TABLE, "");
    }

    static String getCacheDataLocationBlacklist(Configuration configuration)
    {
        return configuration.get(DATA_CACHE_LOCATION_BLACKLIST, "");
    }

    static int getCacheDataMinColumns(Configuration c)
    {
        return c.getInt(DATA_CACHE_TABLE_MIN_COLS, 0);
    }

    static void setCacheDataChosenColumns(Configuration c, int chosen)
    {
        c.setInt(DATA_CACHE_TABLE_COLS_CHOSEN, chosen);
    }

    static int getCacheDataChosenColumns(Configuration c)
    {
        return c.getInt(DATA_CACHE_TABLE_COLS_CHOSEN, 0);
    }

    public static boolean isStrictMode(Configuration c)
    {
        return c.getBoolean(DATA_CACHE_STRICT_MODE, false);
    }

    public static boolean isLocalityInfoForwarded(Configuration c)
    {
        return c.getBoolean(LOCALITY_INFO_FORWARDED, false);
    }

    public static void setLocalityInfoForwarded(Configuration c, boolean value)
    {
        c.setBoolean(LOCALITY_INFO_FORWARDED, value);
    }

    public static Set<String> getLocalityInfo(Configuration c, String node, String file)
    {
        return Sets.newHashSet(c.getStringCollection(getLocalityKey(node, file)));
    }

    public static void setLocalityInfo(Configuration c, String node, String file, String splits)
    {
        c.set(getLocalityKey(node, file), splits);
    }

    private static String getLocalityKey(String node, String file)
    {
        return LOCALITY_INFO + node + file;
    }

    // Helper methods to get information based on configuration

    public static boolean skipCache(Path path, Configuration conf)
    {
        if (!CachingConfigHelper.isCacheDataEnabled(conf)) {
            return true;
        }

        if (!isLocationAllowedToCache(path, conf)) {
            return true;
        }

        if (!isTableAllowedToCache(conf)) {
            return true;
        }

        if (!minColumnsSelected(conf)) {
            return true;
        }

        return false;
    }

    private static boolean isTableAllowedToCache(Configuration conf)
    {
        String table = CachingConfigHelper.getCacheDataTable(conf);
        if (table == null || (table != null && table.isEmpty())) {
            // Support not added by engine
            return true;
        }

        String whitelist = CachingConfigHelper.getCacheDataTableWhitelist(conf);
        if (whitelist.length() > 0) {
            if (!table.matches(whitelist)) {
                return false;
            }
        }

        return true;
    }

    private static boolean isLocationAllowedToCache(Path path, Configuration conf)
    {
        String blacklist = CachingConfigHelper.getCacheDataLocationBlacklist(conf);
        if (blacklist.length() > 0) {
            if (path.toString().matches(blacklist)) {
                return false;
            }
        }

        return true;
    }

    private static boolean minColumnsSelected(Configuration conf)
    {
        if (CachingConfigHelper.getCacheDataMinColumns(conf) > CachingConfigHelper
                .getCacheDataChosenColumns(conf)) {
            return false;
        }
        return true;
    }
}
