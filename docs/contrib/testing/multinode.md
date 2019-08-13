# Multi-Node Integration Tests

Since RubiX is used as part of a cluster of nodes, we also need a way to run test scenarios using a cluster.

To accomplish this, we run a set of interconnected Docker containers to mimic a cluster and its state,
and Robot Framework to execute tests with this cluster.

## Robot Framework

When creating multi-node integration tests:
* use the **Multi-node test setup**/**Multi-node test teardown** keywords for setup/teardown. 
* test data must be generated in `/tmp/rubix/tests/`, so it is available for use by the cluster nodes. 
* to check the state of cluster nodes after running a test, comment the **\[Teardown\]** line
  and use `docker exec -it <container-name> /bin/bash` to run a shell on the node you want to check.
    * Logs for daemons will be available in `/home/logs` on the node.

## Docker

The Docker cluster is defined by `rubix-tests/src/test/robotframework/shared/docker/docker-compose.yml`.
(For more information on Docker compose files, check out <https://docs.docker.com/compose/>)

This file defines the state of the cluster nodes when they start for each integration test,
and specifies properties such as:

* whether the node is the master/coordinator node
* which data volumes are linked to the node (in our case, test data and the project JARs)
* the IP address for the node (used for direct communication with the node)

```
...
  rubix-master:
    build:
      context: .
      args:
        is_master: "true"
    volumes:
      - /tmp/rubix/tests:/tmp/rubix/tests
      - /tmp/rubix/jars:/usr/lib/rubix/lib
    networks:
      default:
        ipv4_address: 172.18.8.0
  rubix-worker-1:
    ...
  rubix-worker-2:
    ...
...
```

## ContainerRequestClient/Server

A `CachingFileSystem` needs to be running on the same node as the BookKeeper node it will try to connect to.
This means that we need some way to pass a request from the test to the worker node that will execute it.

We accomplish this using Java RMI (Remote Method Invocation), which lets us connect to a server on a worker node 
and execute methods using that server (for more info on RMI, 
check out [the Java Tutorials for RMI](https://docs.oracle.com/javase/tutorial/rmi/overview.html)).

Worker nodes in the Docker cluster will run a `ContainerRequestServer`, which binds itself to the RMI registry running on that node.
Robot Framework tests call `ContainerRequestClient` methods along with a specific worker's hostname,
which will contact the RMI registry on that worker node to locate the `...Server` and let us run those methods on that server.
