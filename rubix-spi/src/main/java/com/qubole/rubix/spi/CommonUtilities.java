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
package com.qubole.rubix.spi;

import org.apache.hadoop.conf.Configuration;

public class CommonUtilities
{
    private CommonUtilities()
    {
        // Static class
    }

    public static long toBlockStartPosition(long block, Configuration conf)
    {
        return block * CacheConfig.getBlockSize(conf);
    }

    public static long toBlockEndPosition(long block, Configuration conf)
    {
        return toBlockStartPosition(block + 1, conf) - 1;
    }

    public static long alignToBlockStartPosition(long position, Configuration conf)
    {
        return toStartBlock(position, conf) * CacheConfig.getBlockSize(conf);
    }

    public static long alignToBlockEndPosition(long position, Configuration conf)
    {
        return toEndBlock(position, conf) * CacheConfig.getBlockSize(conf);
    }

    public static long toStartBlock(long startingPosition, Configuration conf)
    {
        return toStartBlock(startingPosition, CacheConfig.getBlockSize(conf));
    }

    public static long toEndBlock(long endPosition, Configuration conf)
    {
        return toEndBlock(endPosition, CacheConfig.getBlockSize(conf));
    }

    /*
     * For a request on range [startPosition, endPosition) this method returns the startBlockNumber if we convert
     * the request range to block aligned request as [startBlockNumber, endBlockNumber) i.e. the first block (inclusive)
     * from which the data read has to start
     */
    public static long toStartBlock(long startingPosition, int blockSize)
    {
        return startingPosition / blockSize;
    }

    /*
     * For a request on range [startPosition, endPosition) this method returns the endBlockNumber if we convert
     * the request range to block aligned request as [startBlockNumber, endBlockNumber) i.e. the block number at which
     * the data read has to stop
     */
    public static long toEndBlock(long endPosition, int blockSize)
    {
        return ((endPosition - 1) / blockSize) + 1;
    }
}
