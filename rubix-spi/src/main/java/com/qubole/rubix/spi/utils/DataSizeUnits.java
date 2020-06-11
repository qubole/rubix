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
package com.qubole.rubix.spi.utils;

// On same lines as TimeUnit
public enum DataSizeUnits
{
    BYTES {
        public long toBytes(long size)
        {
            return size;
        }

        public long toKB(long size)
        {
            return size / 1024L;
        }

        public long toMB(long size)
        {
            return toKB(size) / 1024L;
        }

        public long toGB(long size)
        {
            return toMB(size) / 1024L;
        }
    },
    KILOBYTES {
        public long toBytes(long size)
        {
            return size * 1024L;
        }

        public long toKB(long size)
        {
            return size;
        }

        public long toMB(long size)
        {
            return size / 1024L;
        }

        public long toGB(long size)
        {
            return toMB(size) / 1024L;
        }
    },
    MEGABYTES {
        public long toBytes(long size)
        {
            return KILOBYTES.toBytes(toKB(size));
        }

        public long toKB(long size)
        {
            return size * 1024L;
        }

        public long toMB(long size)
        {
            return size;
        }

        public long toGB(long size)
        {
            return size / 1024L;
        }
    },
    GIGABYTES {
        public long toBytes(long size)
        {
            return MEGABYTES.toBytes(toMB(size));
        }

        public long toKB(long size)
        {
            return MEGABYTES.toKB(toMB(size));
        }

        public long toMB(long size)
        {
            return size * 1024L;
        }

        public long toGB(long size)
        {
            return size;
        }
    };

    public long toBytes(long size)
    {
        throw new AbstractMethodError();
    }

    public long toKB(long size)
    {
        throw new AbstractMethodError();
    }

    public long toMB(long size)
    {
        throw new AbstractMethodError();
    }

    public long toGB(long size)
    {
        throw new AbstractMethodError();
    }
}
