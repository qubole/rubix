#!/bin/sh

CONTAINER_IP=$(hostname -i)

export CLASSPATH=/usr/lib/rubix/lib/*:${CLASSPATH}
rmiregistry -J-Djava.rmi.server.logCalls=true -J-Djava.rmi.server.hostname=${CONTAINER_IP} > /tmp/registry.log 2>&1 &

#export HADOOP_OPTS="-Dlog4j.configuration=file:///home/props/log4j_crs.properties -Djava.rmi.server.logCalls=true -Djava.rmi.server.hostname=${CONTAINER_IP}"
#nohup /usr/lib/hadoop/bin/hadoop jar /usr/lib/rubix/lib/rubix-client-*.jar \
#com.qubole.rubix.client.robotframework.container.server.ContainerRequestServer 8123 \
#> /home/logs/start-crs.log 2>&1

echo "Variable (${IS_CLUSTER_MASTER})" > /home/logs/test
if [ "${IS_CLUSTER_MASTER}" = "true" ]; then
  export HADOOP_OPTS="-Dlog4j.configuration=file:///home/props/log4j_crs.properties -Djava.rmi.server.logCalls=true -Djava.rmi.server.hostname=${CONTAINER_IP}"
  nohup /usr/lib/hadoop/bin/hadoop jar /usr/lib/rubix/lib/rubix-client-*.jar \
  com.qubole.rubix.client.robotframework.testdriver.CoordinatorTestDriver \
  > /home/logs/start-ctd.log 2>&1
else
  export HADOOP_OPTS="-Dlog4j.configuration=file:///home/props/log4j_crs.properties -Djava.rmi.server.logCalls=true -Djava.rmi.server.hostname=${CONTAINER_IP}"
  nohup /usr/lib/hadoop/bin/hadoop jar /usr/lib/rubix/lib/rubix-client-*.jar \
  com.qubole.rubix.client.robotframework.testdriver.WorkerTestDriver \
  > /home/logs/start-wtd.log 2>&1
fi
