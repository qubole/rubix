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

import org.testng.annotations.Test;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.List;

import static org.testng.AssertJUnit.assertTrue;

/**
 * Created by stagra on 15/1/16.
 */
public class TestReadRequestChain
{
    @Test
    public void testConsequtiveRequest()
    {
        ReadRequest rr1 = new ReadRequest(0, 1024, 0, 1024, null, 0, 2048);
        ReadRequest rr2 = new ReadRequest(1024, 2048, 1024, 2048, null, 1024, 2048);
        CachedReadRequestChain chain = new CachedReadRequestChain();
        chain.addReadRequest(rr1);
        chain.addReadRequest(rr2);

        List<ReadRequest> finalRequests = chain.getReadRequests();

        assertTrue("Requests not collated", finalRequests.size() == 1);

        ReadRequest rr = finalRequests.get(0);
        assertTrue("Wrong backend read start", rr.getBackendReadStart() == 0);
        assertTrue("Wrong backend read end", rr.getBackendReadEnd() == 2048);
        assertTrue("Wrong actual read start", rr.getActualReadStart() == 0);
        assertTrue("Wrong actual read end", rr.getActualReadEnd() == 2048);

    }
}
