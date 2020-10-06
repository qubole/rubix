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
package com.qubole.rubix.core;

import org.testng.annotations.Test;

import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

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
    chain.lock();

    List<ReadRequest> finalRequests = chain.getReadRequests();

    assertTrue(finalRequests.size() == 1, "Requests not collated");

    ReadRequest rr = finalRequests.get(0);
    assertTrue(rr.getBackendReadStart() == 0, "Wrong backend read start");
    assertTrue(rr.getBackendReadEnd() == 2048, "Wrong backend read end");
    assertTrue(rr.getActualReadStart() == 0, "Wrong actual read start");
    assertTrue(rr.getActualReadEnd() == 2048, "Wrong actual read end");
  }

  @Test
  public void testMaxReadRequestSize()
  {
    ReadRequest rr1 = new ReadRequest(0, 1024, 40, 1024, null, 0, 2500);
    ReadRequest rr2 = new ReadRequest(1024, 1280, 1024, 1280, null, 0, 2500);
    ReadRequest rr3 = new ReadRequest(1280, 1792, 1280, 1792, null, 0, 2500);
    ReadRequest rr4 = new ReadRequest(1792, 2048, 1792, 2000, null, 0, 2500);
    ReadRequest rr5 = new ReadRequest(2100, 2200, 2100, 2200, null, 0, 2500);

    CachedReadRequestChain chain = new CachedReadRequestChain();
    chain.setMaxReadRequestSize(512);
    chain.addReadRequest(rr1);
    chain.addReadRequest(rr2);
    chain.addReadRequest(rr3);
    chain.addReadRequest(rr4);
    chain.addReadRequest(rr5);
    chain.lock();

    // Expected chains
    ReadRequest[] expectedReadRequests = {
            new ReadRequest(0, 512, 40, 512, null, 0, 2500),
            new ReadRequest(512, 1024, 512, 1024, null, 472, 2500),
            new ReadRequest(1024, 1536, 1024, 1536, null, 984, 2500),
            new ReadRequest(1536, 2048, 1536, 2000, null, 1496, 2500),
            new ReadRequest(2100, 2200, 2100, 2200, null, 0, 2500),
    };

    int idx = 0;
    for (ReadRequest expected : expectedReadRequests) {
      assertEquals(chain.getReadRequests().get(idx), expected, String.format("Expected=%s and actual=%s", expected, chain.getReadRequests().get(idx)));
      idx++;
    }
  }
}
