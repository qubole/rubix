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
package com.qubole.rubix.health;

import com.qubole.rubix.spi.BookKeeperFactory;
import com.qubole.rubix.spi.RetryingBookkeeperClient;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.thrift.shaded.TException;
import org.apache.thrift.shaded.transport.TTransportException;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

/**
 * Created by kvankayala on 02 Jan 2019.
 */

public class TestBookKeeperHealth extends Configured
{
  private static final Log log = LogFactory.getLog(TestBookKeeperHealth.class);
  private final Configuration conf = new Configuration();

  @Test
  public void testCheckIfBookKeeperAlive_Postive() throws TException
  {
    final BookKeeperFactory mockBookKeeperFactory = mock(BookKeeperFactory.class);
    final BookKeeperHealth bookKeeperHealth = new BookKeeperHealth(conf, mockBookKeeperFactory);
    final RetryingBookkeeperClient client = mock(RetryingBookkeeperClient.class);

    when(mockBookKeeperFactory.createBookKeeperClient(conf)).thenReturn(client);
    when(client.isBookKeeperAlive()).thenReturn(true);

    boolean ifBookKeeperResponding = bookKeeperHealth.checkIfBookKeeperAlive();
    assertEquals(ifBookKeeperResponding, true, "Bookkeeper health check should return true");
  }

  @Test
  public void testCheckIfBookKeeperAlive_Negative() throws TException
  {
    final BookKeeperFactory mockBookKeeperFactory = mock(BookKeeperFactory.class);
    final BookKeeperHealth bookKeeperHealth = new BookKeeperHealth(conf, mockBookKeeperFactory);
    final RetryingBookkeeperClient client = mock(RetryingBookkeeperClient.class);

    when(mockBookKeeperFactory.createBookKeeperClient(conf)).thenReturn(client);
    when(client.isBookKeeperAlive()).thenThrow(TTransportException.class);

    boolean ifBookKeeperResponding = bookKeeperHealth.checkIfBookKeeperAlive();
    assertEquals(ifBookKeeperResponding, false, "Bookkeeper health check should return false");
  }
}
