#!/bin/bash -e

## TODO Get Kamesh to publish new build image
echo "=== Fetching RubiX build image ==="
docker pull quboleinc/hadoop_mvn_thrift
echo "=== Finished fetching image ==="
#echo "=== Building RubiX build image ==="
#docker build --tag=rubix-build . > docker-rubix-build.log 2>&1
#echo "=== Finished building image ==="

docker network create \
--driver=bridge \
--subnet=172.18.0.0/16 \
${RUBIX_DOCKER_NETWORK_NAME}

RUBIX_TEST_DATA_DIR=/tmp/rubix/tests
mkdir -p ${RUBIX_TEST_DATA_DIR}
