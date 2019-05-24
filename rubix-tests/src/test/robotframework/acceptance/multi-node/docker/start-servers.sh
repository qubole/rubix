#!/bin/bash

export HADOOP_OPTS="-Dlog4j.configuration=file:///tmp/log4j_bks.properties"
nohup /usr/lib/hadoop/bin/hadoop jar /usr/lib/rubix/lib/rubix-bookkeeper.jar \
com.qubole.rubix.bookkeeper.BookKeeperServer \
-Drubix.cluster.is-master=false \
-Dmaster.hostname=172.18.8.100 \
-Drubix.cache.max.size=2 \
-Drubix.cluster.manager.dummy.class=com.qubole.rubix.core.utils.DockerTestClusterManager \
-Drubix.cache.filesplit.size=1048576 \
> /tmp/cbk.log 2>&1 &


export HADOOP_OPTS="-Dlog4j.configuration=file:///tmp/log4j_lds.properties"
nohup /usr/lib/hadoop/bin/hadoop jar /usr/lib/rubix/lib/rubix-bookkeeper.jar \
com.qubole.rubix.bookkeeper.LocalDataTransferServer \
-Drubix.cluster.is-master=false \
-Dmaster.hostname=172.18.8.100 \
-Drubix.cache.max.size=2 \
-Drubix.cluster.manager.dummy.class=com.qubole.rubix.core.utils.DockerTestClusterManager \
-Drubix.cache.filesplit.size=1048576 \
> /tmp/cld.log 2>&1 &


export CLASSPATH=/usr/lib/rubix/lib/*:${CLASSPATH}
rmiregistry -J-Djava.rmi.server.logCalls=true -J-Djava.rmi.server.hostname=127.0.0.1 & > /tmp/registry.log 2>&1

export HADOOP_OPTS="-Dlog4j.configuration=file:///tmp/log4j_rs.properties -Djava.rmi.server.logCalls=true -Djava.rmi.server.hostname=127.0.0.1"
nohup /usr/lib/hadoop/bin/hadoop jar /usr/lib/rubix/lib/rubix-client.jar \
com.qubole.rubix.client.robotframework.driver.server.ContainerRequestServer \
> /tmp/crs.log 2>&1 ; return 0

echo "Done" >> test1
echo "1 2 3" >> test1



#export HADOOP_OPTS="-Dlog4j.configuration=file:///tmp/log4j_lds.properties"
#nohup /usr/lib/hadoop/bin/hadoop jar /usr/lib/rubix/lib/rubix-bookkeeper.jar \
#com.qubole.rubix.bookkeeper.LocalDataTransferServer \
#> /tmp/cld.log 2>&1
