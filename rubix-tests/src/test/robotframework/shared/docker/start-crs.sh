#!/bin/sh

export CLASSPATH=/usr/lib/rubix/lib/*:${CLASSPATH}
rmiregistry -J-Djava.rmi.server.logCalls=true -J-Djava.rmi.server.hostname=127.0.0.1 > /tmp/registry.log 2>&1 &

export HADOOP_OPTS="-Dlog4j.configuration=file:///home/props/log4j_crs.properties -Djava.rmi.server.logCalls=true -Djava.rmi.server.hostname=127.0.0.1"
nohup /usr/lib/hadoop/bin/hadoop jar /usr/lib/rubix/lib/rubix-client.jar \
com.qubole.rubix.client.robotframework.container.server.ContainerRequestServer 8123 \
> /home/logs/start-crs.log 2>&1
