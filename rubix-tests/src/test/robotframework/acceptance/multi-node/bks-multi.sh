#!/bin/bash -e

copy-jars() {
  SCRIPT_DIR=$1

  RUBIX_JARS=`ls rubix-*/target/rubix-*.jar | grep -E -v 'tests|client|rpm|presto'`
  cp ${RUBIX_JARS} ${SCRIPT_DIR}/docker/jars/

  RUBIX_CLIENT_TEST_JAR=`ls rubix-client/target/rubix-client-*-tests.jar`
  cp ${RUBIX_CLIENT_TEST_JAR} ${SCRIPT_DIR}/docker/jars/

  RUBIX_CORE_TEST_JAR=`ls rubix-core/target/rubix-core-*-tests.jar`
  cp ${RUBIX_CORE_TEST_JAR} ${SCRIPT_DIR}/docker/jars/rubix-core_tests.jar
}

start-multi() {
  SCRIPT_DIR=$(dirname "$0")

  rm ${SCRIPT_DIR}/docker/jars/* ||:
  mkdir -p ${SCRIPT_DIR}/docker/jars
  echo $(pwd)

  copy-jars ${SCRIPT_DIR}

  docker-compose -f ${SCRIPT_DIR}/docker/docker-compose.yml up -d --build

  sleep 3
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
