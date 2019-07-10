#!/bin/bash -e

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
rubix-build /bin/bash -c "./script.sh ; tail -f /dev/null"
#-e "HOST_REPO_DIR=${PWD}" \
#--ip=172.18.0.1 \
#-p 8910:8901 -p 8810:8801 -p 8110:8123 -p 1910:1901 \
