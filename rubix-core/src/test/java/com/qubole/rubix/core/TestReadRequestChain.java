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

import java.util.List;

import static org.testng.Assert.assertTrue;

/**
 * Created by stagra on 15/1/16.
 */
public class TestReadRequestChain
{
  @Test
  public void testConsecutiveRequest()
  {
    ReadRequest rr1 = new ReadRequest(0, 1024, 0, 1024, null, 0, 2048);
    ReadRequest rr2 = new ReadRequest(1024, 2048, 1024, 2048, null, 1024, 2048);
    CachedReadRequestChain chain = new CachedReadRequestChain();
    chain.addReadRequest(rr1);
    chain.addReadRequest(rr2);

    List<ReadRequest> finalRequests = chain.getReadRequests();

    assertTrue(finalRequests.size() == 1, "Requests not collated");

    ReadRequest rr = finalRequests.get(0);
    assertTrue(rr.getBackendReadStart() == 0, "Wrong backend read start");
    assertTrue(rr.getBackendReadEnd() == 2048, "Wrong backend read end");
    assertTrue(rr.getActualReadStart() == 0, "Wrong actual read start");
    assertTrue(rr.getActualReadEnd() == 2048, "Wrong actual read end");
  }

  @Test
  /*
   * Tests that non-consecutive read requests are correctly handled & not collated.
   */
  public void testNonConsecutiveRequest()
  {
    final ReadRequest rr1 = new ReadRequest(0, 1024, 0, 1000, null, 0, 2048);
    final ReadRequest rr2 = new ReadRequest(2048, 3072, 2500, 3000, null, 1024, 2048);
    final CachedReadRequestChain chain = new CachedReadRequestChain();
    chain.addReadRequest(rr1);
    chain.addReadRequest(rr2);

    final List<ReadRequest> finalRequests = chain.getReadRequests();

    assertTrue(finalRequests.size() == 2, "Requests not distinct");

    ReadRequest rr;
    rr = finalRequests.get(0);
    assertTrue(rr.getBackendReadStart() == 0, "Wrong backend read start");
    assertTrue(rr.getBackendReadEnd() == 1024, "Wrong backend read end");
    assertTrue(rr.getActualReadStart() == 0, "Wrong actual read start");
    assertTrue(rr.getActualReadEnd() == 1000, "Wrong actual read end");

    rr = finalRequests.get(1);
    assertTrue(rr.getBackendReadStart() == 2048, "Wrong read [1] start");
    assertTrue(rr.getBackendReadEnd() == 3072, "Wrong [1] backend read end");
    assertTrue(rr.getActualReadStart() == 2500, "Wrong [1] actual read start");
    assertTrue(rr.getActualReadEnd() == 3000, "Wrong [1] actual read end");
  }
}
