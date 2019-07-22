#!/bin/bash -e

echo "Building RubiX build image"
#docker pull quboleinc/hadoop_mvn_thrift
docker build --tag=rubix-build . > docker-rubix-build.log 2>&1
echo "Finished building image"

docker network create \
--driver=bridge \
--subnet=172.18.0.0/16 \
network-rubix-build
