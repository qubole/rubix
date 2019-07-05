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
package com.qubole.rubix.miniclient;

import com.qubole.rubix.client.robotframework.container.client.GetCacheMetricsRequest;
import com.qubole.rubix.client.robotframework.container.server.RequestServer;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Map;

public class SimpleGCMClient
{
  private SimpleGCMClient()
  {
  }

  public static void main(String[] args)
  {
    String rmiHost = args[0];
    int rmiPort = Integer.parseInt(args[1]);
    int rmiConnectType = Integer.parseInt(args[2]);

    try {
      RequestServer server = null;

      switch (rmiConnectType) {
        case 1:
          server = lookupRequestServer(rmiHost, rmiPort);
          break;
        case 2:
          server = namingLookupRequestServer(rmiHost, rmiPort);
          break;
        default:
          throw new IllegalArgumentException("Wrong connect type specified");
      }

      Map<String, Double> metrics = server.getCacheMetrics(new GetCacheMetricsRequest());
      for (Map.Entry<String, Double> metric : metrics.entrySet()) {
        System.out.println(String.format("%s : %.2f", metric.getKey(), metric.getValue()));
      }
    }
    catch (RemoteException | NotBoundException | MalformedURLException e) {
      System.err.println("SimpleGCMClient exception:");
      e.printStackTrace();
    }
  }

  private static RequestServer namingLookupRequestServer(String host, int port) throws RemoteException, NotBoundException, MalformedURLException
  {
    RequestServer server = (RequestServer) Naming.lookup("rmi://" + host + ":" + port + "/ContainerRequestServer");
    return server;
  }

  private static RequestServer lookupRequestServer(String host, int port) throws RemoteException, NotBoundException
  {
    Registry registry = LocateRegistry.getRegistry(host, port);
    System.out.println("Found registry!");
    return (RequestServer) registry.lookup("ContainerRequestServer");
  }
}
