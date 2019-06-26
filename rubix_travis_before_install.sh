#!/bin/bash -e

echo "Building RubiX build image"
#docker pull quboleinc/hadoop_mvn_thrift
docker build --tag=rubix-build . > docker-rubix-build.log 2>&1
echo "Finished building image"