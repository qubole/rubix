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

import java.nio.MappedByteBuffer;

/**
 * Created by stagra on 29/12/15.
 */

// This class provides bitmap semantics over MappeByteBuffer
public class MappedByteBufferBitmap
{
    final MappedByteBuffer mbuf;

    public MappedByteBufferBitmap(MappedByteBuffer mbuf)
    {
        this.mbuf = mbuf;
    }

    // keeping idx in int as mbuf.get can take only Int. And Integer.MAX_VALUE large enough to keep us safe for big files
    public boolean isSet(int idx)
    {
        byte containerByte = mbuf.get(idx / 8);
        int offset = idx % 8;
        if (((containerByte & ( 1 << offset)) != 0)) {
            return true;
        }

        return false;
    }

    public void set(int idx)
    {
        byte containerByte = mbuf.get(idx / 8);
        int offset = idx % 8;
        mbuf.put(idx/ 8, (byte) (containerByte | ( 1 << offset)));
    }

    public void unset(int idx)
    {
        byte containerByte = mbuf.get(idx / 8);
        int offset = idx % 8;
        mbuf.put(idx/ 8, (byte) (containerByte & ~( 1 << offset)));
    }

    public MappedByteBuffer getMappedByteBuffer()
    {
        return mbuf;
    }
}
