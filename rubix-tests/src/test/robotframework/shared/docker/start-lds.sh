#!/bin/sh

export HADOOP_OPTS="-Dlog4j.configuration=file:///home/props/log4j_lds.properties"
nohup /usr/lib/hadoop/bin/hadoop jar /usr/lib/rubix/lib/rubix-bookkeeper.jar \
com.qubole.rubix.bookkeeper.LocalDataTransferServer \
-Drubix.cluster.is-master=${IS_CLUSTER_MASTER} \
-Dmaster.hostname=172.18.8.0 \
-Drubix.cluster.manager.dummy.class=com.qubole.rubix.core.utils.DockerTestClusterManager \
> /home/logs/start-lds.log 2>&1
