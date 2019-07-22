#!/bin/bash -e

MAVEN_CMD="mvn clean install"

RUBIX_TEST_DATA_DIR=/tmp/rubixTests
mkdir -p ${RUBIX_TEST_DATA_DIR}

docker run -it --rm \
--network=network-rubix-build \
-v /var/run/docker.sock:/var/run/docker.sock \
-v "$PWD":/usr/src/rubix \
-v "$HOME/.m2":/root/.m2 \
-v "${RUBIX_TEST_DATA_DIR}:/tmp/rubixTests" \
-e "HOST_TEST_DATA_DIR=${RUBIX_TEST_DATA_DIR}" \
$ci_env \
-w /usr/src/rubix \
rubix-build /bin/bash -c "./docker_build_rubix.sh ${MAVEN_CMD}"
