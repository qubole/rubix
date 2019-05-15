#!/bin/bash -e

start-multi() {
  SCRIPT_DIR=$(dirname "$0")

  rm ${SCRIPT_DIR}/docker/jars/* ||:
  mkdir -p ${SCRIPT_DIR}/docker/jars
  echo $(pwd)

  RUBIX_JARS=`ls rubix-*/target/rubix-*.jar | grep -E -v 'tests|client|rpm|presto'`
  cp ${RUBIX_JARS} ${SCRIPT_DIR}/docker/jars/

  RUBIX_CLIENT_TEST_JAR=`ls rubix-client/target/rubix-*-tests.jar`
  cp ${RUBIX_CLIENT_TEST_JAR} ${SCRIPT_DIR}/docker/jars/


  docker-compose -f ${SCRIPT_DIR}/docker/docker-compose.yml up -d --build

  sleep 1
}

stop-multi() {
  SCRIPT_DIR=$(dirname "$0")

  docker-compose -f ${SCRIPT_DIR}/docker/docker-compose.yml down
}

cmd=$1
case "$cmd" in
  start-multi) shift ; start-multi;;
  stop-multi) shift ; stop-multi;;
esac

exit 0;
