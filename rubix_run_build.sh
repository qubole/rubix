#!/bin/bash -e

MAVEN_CMD="$@"

echo "=== Starting Docker container for build (cmd: ${MAVEN_CMD})==="
docker run -i --rm \
--network=network-rubix-build \
--volume /var/run/docker.sock:/var/run/docker.sock \
--volume "$PWD":/usr/src/rubix \
--volume "$HOME/.m2":/root/.m2 \
--volume "/tmp/rubix:/tmp/rubix" \
--workdir /usr/src/rubix \
quboleinc/hadoop_mvn_thrift /bin/bash -c "./docker_build_rubix.sh ${MAVEN_CMD}"
