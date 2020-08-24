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
package com.qubole.rubix.metrics;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.qubole.rubix.spi.BookKeeperFactory;
import com.qubole.rubix.spi.RetryingPooledBookkeeperClient;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import java.util.Map;

public class BookKeeperMetricsClient
{
    private static Configuration conf = new Configuration();
    private static BookKeeperFactory factory;
    private static final Log log = LogFactory.getLog(BookKeeperMetricsClient.class);

    public BookKeeperMetricsClient(Configuration conf)
    {
        this(conf, new BookKeeperFactory());
    }

    public BookKeeperMetricsClient(Configuration conf, BookKeeperFactory factory)
    {
        this.conf = conf;
        this.factory = factory;
    }

    public static void main(String[] args)
    {
        BookKeeperMetricsClient metricsClient = new BookKeeperMetricsClient(conf);
        Gson gson = new GsonBuilder().serializeSpecialFloatingPointValues().create();
        System.out.println(gson.toJson(metricsClient.getMetrics()));
    }

    public static Map<String, Double> getMetrics()
    {
        try (RetryingPooledBookkeeperClient rclient = factory.createBookKeeperClient(conf))
        {
            return rclient.getCacheMetrics();
        }
        catch (Exception e)
        {
            log.error("Bookkeeper is not responding", e);
        }
        return null;
    }
}
