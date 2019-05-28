#!/bin/bash

is_master=$1

# BookKeeper server
export HADOOP_OPTS="-Dlog4j.configuration=file:///tmp/log4j_bks.properties"
nohup /usr/lib/hadoop/bin/hadoop jar /usr/lib/rubix/lib/rubix-bookkeeper.jar \
com.qubole.rubix.bookkeeper.BookKeeperServer \
-Drubix.cluster.is-master=${is_master} \
-Dmaster.hostname=172.18.8.0 \
-Drubix.cluster.manager.dummy.class=com.qubole.rubix.core.utils.DockerTestClusterManager \
> /tmp/cbk.log 2>&1 &

# Local Data Transfer server
export HADOOP_OPTS="-Dlog4j.configuration=file:///tmp/log4j_lds.properties"
nohup /usr/lib/hadoop/bin/hadoop jar /usr/lib/rubix/lib/rubix-bookkeeper.jar \
com.qubole.rubix.bookkeeper.LocalDataTransferServer \
-Drubix.cluster.is-master=${is_master} \
-Dmaster.hostname=172.18.8.0 \
-Drubix.cluster.manager.dummy.class=com.qubole.rubix.core.utils.DockerTestClusterManager \
> /tmp/cld.log 2>&1 &

# RMI registry and container request server
export CLASSPATH=/usr/lib/rubix/lib/*:${CLASSPATH}
rmiregistry -J-Djava.rmi.server.logCalls=true & > /tmp/registry.log 2>&1

export HADOOP_OPTS="-Dlog4j.configuration=file:///tmp/log4j_rs.properties -Djava.rmi.server.logCalls=true"
nohup /usr/lib/hadoop/bin/hadoop jar /usr/lib/rubix/lib/rubix-client.jar \
com.qubole.rubix.client.robotframework.driver.server.ContainerRequestServer \
> /tmp/crs.log 2>&1
