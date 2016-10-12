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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.transport.TTransportException;

/**
 * Created by sakshia on 5/10/16.
 */
public class BookKeeperFactory
{
    BookKeeperService.Iface bookKeeper = null;
    private static Log log = LogFactory.getLog(BookKeeperFactory.class.getName());

    public BookKeeperFactory()
    {
    }

    public BookKeeperFactory(BookKeeperService.Iface bookKeeper)
    {
        this.bookKeeper = bookKeeper;
    }
    public BookKeeperClient createBookKeeperClient(Configuration conf)
            throws TTransportException
    {
        if (bookKeeper == null) {
            log.info("Creating client");
            return BookKeeperClient.createBookKeeperClient(conf);
        }
        else {
            log.info("Creating Local Bclient");
            return LocalBookKeeperClient.createBookKeeperClient(conf, bookKeeper);
        }
    }
}
