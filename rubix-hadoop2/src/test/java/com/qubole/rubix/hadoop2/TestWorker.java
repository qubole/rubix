/**
 * Copyright (c) 2018. Qubole Inc
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

package com.qubole.rubix.hadoop2;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Created by Abhishek on 7/3/18.
 */
public class TestWorker implements HttpHandler
{
  private String nodeResponse;

  public TestWorker(String nodeJson)
  {
    this.nodeResponse = nodeJson;
  }

  public TestWorker()
  {
  }

  @Override
  public void handle(HttpExchange exchange) throws IOException
  {
    exchange.getResponseHeaders().add("Content-Type", "application/json");
    exchange.sendResponseHeaders(200, nodeResponse.length());
    final OutputStream os = exchange.getResponseBody();
    os.write(nodeResponse.getBytes());
    os.close();
  }

  /**
   * Http response handler to represent a cluster with multiple running worker nodes.
   */
  class MultipleRunningWorkers extends TestWorker
  {
    public MultipleRunningWorkers()
    {
      super("{\"nodes\":{\"node\":[{\"nodeHostName\":\"" + TestHadoop2ClusterManagerUtil.WORKER_HOSTNAME_2 + "\",\"state\":\"RUNNING\"}," +
          "{\"nodeHostName\":\"" + TestHadoop2ClusterManagerUtil.WORKER_HOSTNAME_1 + "\",\"state\":\"RUNNING\"}]}}\n");
    }
  }
  /**
   * Http response handler to represent a cluster with one decommissioning worker node.
   */
  class MultipleWorkersOneDecommissioned extends TestWorker
  {
    public MultipleWorkersOneDecommissioned()
    {
      super("{\"nodes\":{\"node\":[{\"nodeHostName\":\"" + TestHadoop2ClusterManagerUtil.WORKER_HOSTNAME_2 + "\",\"state\":\"RUNNING\"}," +
          "{\"nodeHostName\":\"" + TestHadoop2ClusterManagerUtil.WORKER_HOSTNAME_1 + "\",\"state\":\"DECOMMISSIONED\"}]}}\n");
    }
  }

  /**
   * Http response handler to represent a cluster with one decommissioning worker node.
   */
  class MultipleWorkersOneDecommissioning extends TestWorker
  {
    public MultipleWorkersOneDecommissioning()
    {
      super("{\"nodes\":{\"node\":[{\"nodeHostName\":\"" + TestHadoop2ClusterManagerUtil.WORKER_HOSTNAME_2 + "\",\"state\":\"RUNNING\"}," +
          "{\"nodeHostName\":\"" + TestHadoop2ClusterManagerUtil.WORKER_HOSTNAME_1 + "\",\"state\":\"DECOMMISSIONING\"}]}}\n");
    }
  }

  /**
   * Http response handler to represent a cluster with one decommissioning worker node.
   */
  class MultipleWorkersOneLost extends TestWorker
  {
    public MultipleWorkersOneLost()
    {
      super("{\"nodes\":{\"node\":[{\"nodeHostName\":\"" + TestHadoop2ClusterManagerUtil.WORKER_HOSTNAME_2 + "\",\"state\":\"RUNNING\"}," +
          "{\"nodeHostName\":\"" + TestHadoop2ClusterManagerUtil.WORKER_HOSTNAME_1 + "\",\"state\":\"LOST\"}]}}\n");
    }
  }

  /**
   * Http response handler to represent a cluster with one decommissioning worker node.
   */
  class MultipleWorkersOneNew extends TestWorker
  {
    public MultipleWorkersOneNew()
    {
      super("{\"nodes\":{\"node\":[{\"nodeHostName\":\"" + TestHadoop2ClusterManagerUtil.WORKER_HOSTNAME_2 + "\",\"state\":\"RUNNING\"}," +
          "{\"nodeHostName\":\"" + TestHadoop2ClusterManagerUtil.WORKER_HOSTNAME_1 + "\",\"state\":\"NEW\"}]}}\n");
    }
  }

  /**
   * Http response handler to represent a cluster with one rebooted worker node.
   */
  class MultipleWorkersOneRebooted extends TestWorker
  {
    public MultipleWorkersOneRebooted()
    {
      super("{\"nodes\":{\"node\":[{\"nodeHostName\":\"" + TestHadoop2ClusterManagerUtil.WORKER_HOSTNAME_2 + "\",\"state\":\"RUNNING\"}," +
          "{\"nodeHostName\":\"" + TestHadoop2ClusterManagerUtil.WORKER_HOSTNAME_1 + "\",\"state\":\"REBOOTED\"}]}}\n");
    }
  }

  /**
   * Http response handler to represent a cluster with one unhealthy worker node.
   */
  class MultipleWorkersOneUnhealthy extends TestWorker
  {
    public MultipleWorkersOneUnhealthy()
    {
      super("{\"nodes\":{\"node\":[{\"nodeHostName\":\"" + TestHadoop2ClusterManagerUtil.WORKER_HOSTNAME_2 + "\",\"state\":\"RUNNING\"}," +
          "{\"nodeHostName\":\"" + TestHadoop2ClusterManagerUtil.WORKER_HOSTNAME_1 + "\",\"state\":\"UNHEALTHY\"}]}}\n");
    }
  }

  /**
   * Http response handler to represent a cluster with no worker nodes.
   */
  class NoWorkers extends TestWorker
  {
    public NoWorkers()
    {
      super("{\"nodes\":{\"node\":[]}}\n");
    }
  }

  /**
   * Http response handler to represent a cluster with one running worker node.
   */
  class OneRunningWorker extends TestWorker
  {
    public OneRunningWorker()
    {
      super("{\"nodes\":{\"node\":[{\"nodeHostName\":\"" + TestHadoop2ClusterManagerUtil.WORKER_HOSTNAME_1 + "\",\"state\":\"RUNNING\"}]}}");
    }
  }

  class FourWorkers extends TestWorker
  {
    public FourWorkers()
    {
      super("{\"nodes\":{\"node\":[{\"nodeHostName\":\"" + TestHadoop2ClusterManagerUtil.WORKER_HOSTNAME_1 + "\",\"state\":\"RUNNING\"}," +
          "{\"nodeHostName\":\"" + TestHadoop2ClusterManagerUtil.WORKER_HOSTNAME_2 + "\",\"state\":\"RUNNING\"}, " +
          "{\"nodeHostName\":\"" + TestHadoop2ClusterManagerUtil.WORKER_HOSTNAME_3 + "\",\"state\":\"RUNNING\"}, " +
          "{\"nodeHostName\":\"" + TestHadoop2ClusterManagerUtil.WORKER_HOSTNAME_4 + "\",\"state\":\"RUNNING\"}]}}\n");
    }
  }

  class FourLiveWorkersOneDecommissioned extends TestWorker
  {
    public FourLiveWorkersOneDecommissioned()
    {
      super("{\"nodes\":{\"node\":[{\"nodeHostName\":\"" + TestHadoop2ClusterManagerUtil.WORKER_HOSTNAME_1 + "\",\"state\":\"DECOMMISSIONED\"}," +
          "{\"nodeHostName\":\"" + TestHadoop2ClusterManagerUtil.WORKER_HOSTNAME_2 + "\",\"state\":\"RUNNING\"}, " +
          "{\"nodeHostName\":\"" + TestHadoop2ClusterManagerUtil.WORKER_HOSTNAME_3 + "\",\"state\":\"RUNNING\"}, " +
          "{\"nodeHostName\":\"" + TestHadoop2ClusterManagerUtil.WORKER_HOSTNAME_4 + "\",\"state\":\"RUNNING\"}, " +
          "{\"nodeHostName\":\"" + TestHadoop2ClusterManagerUtil.WORKER_HOSTNAME_5 + "\",\"state\":\"RUNNING\"}]}}\n");
    }
  }

  class SixWorkers extends TestWorker
  {
    public SixWorkers()
    {
      super("{\"nodes\":{\"node\":[{\"nodeHostName\":\"" + TestHadoop2ClusterManagerUtil.WORKER_HOSTNAME_1 + "\",\"state\":\"RUNNING\"}," +
          "{\"nodeHostName\":\"" + TestHadoop2ClusterManagerUtil.WORKER_HOSTNAME_2 + "\",\"state\":\"RUNNING\"}, " +
          "{\"nodeHostName\":\"" + TestHadoop2ClusterManagerUtil.WORKER_HOSTNAME_3 + "\",\"state\":\"RUNNING\"}, " +
          "{\"nodeHostName\":\"" + TestHadoop2ClusterManagerUtil.WORKER_HOSTNAME_4 + "\",\"state\":\"RUNNING\"}, " +
          "{\"nodeHostName\":\"" + TestHadoop2ClusterManagerUtil.WORKER_HOSTNAME_5 + "\",\"state\":\"RUNNING\"}, " +
          "{\"nodeHostName\":\"" + TestHadoop2ClusterManagerUtil.WORKER_HOSTNAME_6 + "\",\"state\":\"RUNNING\"}]}}\n");
    }
  }

  class FourLiveWorkersTwoDecommissioned extends TestWorker
  {
    public FourLiveWorkersTwoDecommissioned()
    {
      super("{\"nodes\":{\"node\":[{\"nodeHostName\":\"" + TestHadoop2ClusterManagerUtil.WORKER_HOSTNAME_1 + "\",\"state\":\"DECOMMISSIONED\"}," +
          "{\"nodeHostName\":\"" + TestHadoop2ClusterManagerUtil.WORKER_HOSTNAME_2 + "\",\"state\":\"RUNNING\"}, " +
          "{\"nodeHostName\":\"" + TestHadoop2ClusterManagerUtil.WORKER_HOSTNAME_3 + "\",\"state\":\"RUNNING\"}, " +
          "{\"nodeHostName\":\"" + TestHadoop2ClusterManagerUtil.WORKER_HOSTNAME_4 + "\",\"state\":\"RUNNING\"}, " +
          "{\"nodeHostName\":\"" + TestHadoop2ClusterManagerUtil.WORKER_HOSTNAME_5 + "\",\"state\":\"RUNNING\"}, " +
          "{\"nodeHostName\":\"" + TestHadoop2ClusterManagerUtil.WORKER_HOSTNAME_6 + "\",\"state\":\"DECOMMISSIONED\"}]}}\n");
    }
  }
}
