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

import org.apache.commons.pool2.ObjectPool;
import static org.testng.AssertJUnit.assertTrue;

import org.apache.commons.pool2.impl.SoftReferenceObjectPool;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Vamshi on 2/21/17.
 */
public class TestReadRequestPool {
    @Test
    public void test() throws Exception {
        ObjectPool<ReadRequest> readRequestPool = new SoftReferenceObjectPool<>(new ReadRequestFactory());
        List<ReadRequest> list = new ArrayList<>();
        int totalObjects = 40;
        for (int i = 0; i < totalObjects; i++) {
            list.add(readRequestPool.borrowObject());
        }
        int numActive;
        int numIdle;
        numActive = readRequestPool.getNumActive();
        numIdle = readRequestPool.getNumIdle();
        assertTrue("Wrong number of active objects", numActive==totalObjects);
        assertTrue("Wrong number of idle objects", numIdle == 0);
        int releaseObjects = 21;
        for (int i = 0; i < releaseObjects; i++) {
            readRequestPool.returnObject(list.get(i));
        }
        numActive = readRequestPool.getNumActive();
        numIdle = readRequestPool.getNumIdle();
        assertTrue("Wrong number of active objects after releasing", numActive == (totalObjects-releaseObjects));
        assertTrue("Wrong number of idle objects after releasing", numIdle == releaseObjects);
    }
}
