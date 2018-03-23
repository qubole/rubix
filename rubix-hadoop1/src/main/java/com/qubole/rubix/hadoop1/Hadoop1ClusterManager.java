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
package com.qubole.rubix.hadoop1;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.qubole.rubix.spi.ClusterManager;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Created by stagra on 9/2/16.
 */
public class Hadoop1ClusterManager extends ClusterManager
{
  private boolean isMaster = true;
  Supplier<List<String>> nodesSupplier;
  private int nnPort = 50070;

  public static String nnPortConf = "caching.fs.namenode-port";

  private static final Log LOG = LogFactory.getLog(Hadoop1ClusterManager.class);

  @Override
  public void initialize(Configuration conf)
  {
    super.initialize(conf);
    nnPort = conf.getInt(nnPortConf, nnPort);

    nodesSupplier = Suppliers.memoizeWithExpiration(new Supplier<List<String>>()
    {
      @Override
      public List<String> get()
      {
        if (!isMaster) {
          // First time all nodes start assuming themselves as master and down the line figure out their role
          // Next time onwards, only master will be fetching the list of nodes
          return ImmutableList.of();
        }

        HttpClient httpclient = new HttpClient();
        HttpMethod method = new GetMethod("http://localhost:" + nnPort + "/dfsnodelist.jsp?whatNodes=LIVE&status=NORMAL");
        int sc;
        try {
          sc = httpclient.executeMethod(method);
        }
        catch (IOException e) {
          // not reachable => worker
          sc = -1;
        }

        if (sc != 200) {
          LOG.debug("Could not reach dfsnodelist.jsp, setting worker role");
          isMaster = false;
          return ImmutableList.of();
        }

        LOG.debug("Reached dfsnodelist.jsp, setting master role");
        isMaster = true;
        String html;
        try {
          byte[] buf = method.getResponseBody();
          html = new String(buf);
        }
        catch (IOException e) {
          throw Throwables.propagate(e);
        }

        List<String> nodes = extractNodes(html);
        return nodes;
      }
    }, 10, TimeUnit.SECONDS);
    nodesSupplier.get();
  }

  private List<String> extractNodes(String dfsnodelist)
  {
    Document doc = Jsoup.parse(dfsnodelist);

    String title = doc.title();
    List<String> workers = new ArrayList<String>();

    Elements links = doc.select(".name");
    for (int i = 0; i < links.size(); i++) {
      Elements nodes = links.get(i).select("a[href]");
      if (nodes != null && nodes.size() > 0) {
        String node = nodes.get(0).ownText();
        if (node != null && !node.isEmpty()) {
          workers.add(node);
        }
      }
    }

    Collections.sort(workers);
    return workers;
  }

  @Override
  public List<String> getNodes()
  {
    long start = System.nanoTime();
    List<String> nodes = nodesSupplier.get();
    LOG.info("Fetched node list in " + (System.nanoTime() - start) / Math.pow(10, 9) + "sec");
    return nodes;
  }

  @Override
  public boolean isMaster()
  {
    // issue get on nodesSupplier to ensure that isMaster is set correctly
    nodesSupplier.get();
    return isMaster;
  }
}
