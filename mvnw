#!/bin/bash

RUBIX_DOCKER_NETWORK=network-rubix-build
RUBIX_DOCKER_TEST_DATA_DIR=/tmp/rubixTests

initialize-docker-container() {
    echo "=== Building RubiX build Docker image ==="
#    echo "=== Fetching RubiX build Docker image ==="
#    docker pull quboleinc/hadoop_mvn_thrift
    docker build --tag=rubix-build . #> docker-rubix-build.log 2>&1

    docker network create \
    --driver=bridge \
    --subnet=172.18.0.0/16 \
    ${RUBIX_DOCKER_NETWORK}

    mkdir -p ${RUBIX_DOCKER_TEST_DATA_DIR}
}

start-docker-build() {
    MAVEN_CMD=$1

    initialize-docker-container

    echo "=== Starting Docker container for build ==="
    docker run -it --rm \
    --network=${RUBIX_DOCKER_NETWORK} \
    --env "HOST_TEST_DATA_DIR=${RUBIX_DOCKER_TEST_DATA_DIR}" \
    --volume /var/run/docker.sock:/var/run/docker.sock \
    --volume "$PWD":/usr/src/rubix \
    --volume "$HOME/.m2":/root/.m2 \
    --volume "${RUBIX_DOCKER_TEST_DATA_DIR}:/tmp/rubixTests" \
    --workdir /usr/src/rubix \
    rubix-build /bin/bash -c "./docker_build_rubix.sh \"${MAVEN_CMD}\""
}

MAVEN_CMD="mvn $@"

if [[ "$*" == *-DrunITs* ]]
then
    start-docker-build "${MAVEN_CMD}"
else
    exec ${MAVEN_CMD} -Pno-integration-tests
fi
