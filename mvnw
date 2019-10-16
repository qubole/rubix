#!/bin/bash

MAVEN_CMD="mvn $@"

if [[ "$*" == *-Pintegration-tests* ]]
then
    export RUBIX_DOCKER_NETWORK_NAME=network-rubix-build
    source ./rubix_setup_build.sh
    source ./rubix_run_build.sh "${MAVEN_CMD}"
else
    exec ${MAVEN_CMD}
fi
