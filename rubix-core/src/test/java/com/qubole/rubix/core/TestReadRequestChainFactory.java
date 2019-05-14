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

import org.apache.hadoop.conf.Configuration;
import org.testng.annotations.Test;

import java.io.IOException;

import static org.testng.Assert.assertEquals;

public class TestReadRequestChainFactory
{
  @Test
  public void testReadRequestChainFactory() throws IOException
  {
    assertEquals(ReadRequestChainFactory.createReadRequestChain(DirectReadRequestChain.class, null).getClass(), DirectReadRequestChain.class);

    assertEquals(ReadRequestChainFactory.createReadRequestChain(CachedReadRequestChain.class, null, null, null, null,
             null, null).getClass(), CachedReadRequestChain.class);

    assertEquals(ReadRequestChainFactory.createReadRequestChain(NonLocalReadRequestChain.class, null, 0, 0,
             null, null, null, 0, false, null, 0, 0).getClass(), NonLocalReadRequestChain.class);

    assertEquals(ReadRequestChainFactory.createReadRequestChain(RemoteFetchRequestChain.class, null, 0, 0,
            null, null, null, 0, false, null, 0, 0).getClass(), RemoteFetchRequestChain.class);

    assertEquals(ReadRequestChainFactory.createReadRequestChain(RemoteReadRequestChain.class, null, null, null,
            new byte[0], null).getClass(), RemoteReadRequestChain.class);

    assertEquals(ReadRequestChainFactory.createReadRequestChain(FileDownloadRequestChain.class, null, null, null,
            null, new Configuration(), null, 0, 0).getClass(), FileDownloadRequestChain.class);
  }
}
