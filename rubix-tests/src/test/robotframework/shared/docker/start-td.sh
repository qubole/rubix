#!/bin/sh

CONTAINER_IP=$(hostname -i)

export CLASSPATH=/usr/lib/rubix/lib/*:${CLASSPATH}
rmiregistry -J-Djava.rmi.server.logCalls=true -J-Djava.rmi.server.hostname=${CONTAINER_IP} > /tmp/registry.log 2>&1 &

if [ "${IS_CLUSTER_MASTER}" = "true" ]; then
  export HADOOP_OPTS="-Dlog4j.configuration=file:///home/props/log4j_ctd.properties -Djava.rmi.server.logCalls=true -Djava.rmi.server.hostname=${CONTAINER_IP}"
  nohup /usr/lib/hadoop/bin/hadoop jar /usr/lib/rubix/lib/rubix-client-*.jar \
  com.qubole.rubix.client.robotframework.testdriver.CoordinatorTestDriver \
  > /home/logs/start-ctd.log 2>&1
else
  export HADOOP_OPTS="-Dlog4j.configuration=file:///home/props/log4j_wtd.properties -Djava.rmi.server.logCalls=true -Djava.rmi.server.hostname=${CONTAINER_IP}"
  nohup /usr/lib/hadoop/bin/hadoop jar /usr/lib/rubix/lib/rubix-client-*.jar \
  com.qubole.rubix.client.robotframework.testdriver.WorkerTestDriver \
  > /home/logs/start-wtd.log 2>&1
fi
