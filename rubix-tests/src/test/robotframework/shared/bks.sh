#!/bin/bash -e

CACHE_DIR_PREFIX_KEY="rubix.cache.dirprefix.list"
CACHE_DIR_SUFFIX_KEY="rubix.cache.dirsuffix"
CACHE_DIR_MAX_DISKS_KEY="rubix.cache.max.disks"
CACHE_DIR_PREFIX_VALUE=/media/ephemeral
CACHE_DIR_SUFFIX_VALUE=/fcache/
CACHE_DIR_MAX_DISKS_VALUE=5

BASE_DIR=`dirname $0`
BASE_DIR=`cd "$BASE_DIR"; pwd`

RUN_DIR=${BASE_DIR}/bks
PID_FILE=${RUN_DIR}/bks.pid
LOG4J_FILE=${RUN_DIR}/log4j.properties

CUR_DATE=$(date '+%Y-%m-%dT%H-%M-%S')
LOG_DIR=${RUN_DIR}/logs
LOG_FILE=${LOG_DIR}/bks-${CUR_DATE}.log
SCRIPT_LOG_FILE=${LOG_DIR}/bks-script.log

HADOOP_DIR=/usr/lib/hadoop2
HADOOP_JAR_DIR=${HADOOP_DIR}/share/hadoop/tools/lib

setup-log4j() {
(cat << EOF

log4j.rootLogger=DEBUG, R

log4j.appender.R=org.apache.log4j.RollingFileAppender
log4j.appender.R.File=${LOG_FILE}
log4j.appender.R.MaxFileSize=100MB
log4j.appender.R.MaxBackupIndex=5
log4j.appender.R.layout=org.apache.log4j.PatternLayout
log4j.appender.R.layout.ConversionPattern=%d{yy/MM/dd HH:mm:ss,SSS} %p %t %c{2}: %m%n

log4j.logger.com.qubole.rubix=DEBUG
log4j.logger.org.apache.hadoop.fs.s3a.S3AFileSystem=DEBUG

EOF
) > ${LOG4J_FILE}
}

set-cache-options() {
  for option in "$@"
  do
    option_key=$(echo "${option}" | sed -n 's/-D\(.*\)=\(.*\)/\1/p')
    option_value=$(echo "${option}" | sed -n 's/-D\(.*\)=\(.*\)/\2/p')
    case "$option_key" in
      ${CACHE_DIR_PREFIX_KEY}) CACHE_DIR_PREFIX_VALUE="${option_value}";;
      ${CACHE_DIR_SUFFIX_KEY}) CACHE_DIR_SUFFIX_VALUE="${option_value}";;
      ${CACHE_DIR_MAX_DISKS_KEY}) CACHE_DIR_MAX_DISKS_VALUE="${option_value}";;
    esac
  done
}

setup-disks() {
  PREFIX=${CACHE_DIR_PREFIX_VALUE}
  SUFFIX=${CACHE_DIR_SUFFIX_VALUE}
  MAX_DISKS=${CACHE_DIR_MAX_DISKS_VALUE}

  for i in $(seq 0 $((MAX_DISKS-1)))
  do
    CACHE_DIR=${PREFIX}${i}${SUFFIX}
    mkdir -p ${CACHE_DIR}
    chmod -R 777 ${CACHE_DIR}
  done
}

copy-jars-for-containers() {
  SCRIPT_DIR=$1

  RUBIX_JARS=`ls rubix-*/target/rubix-*.jar | grep -E -v 'tests|client|rpm|presto'`
  cp ${RUBIX_JARS} ${SCRIPT_DIR}/docker/jars/

  RUBIX_CLIENT_TEST_JAR=`ls rubix-client/target/rubix-client-*-tests.jar`
  cp ${RUBIX_CLIENT_TEST_JAR} ${SCRIPT_DIR}/docker/jars/

  RUBIX_CORE_TEST_JAR=`ls rubix-core/target/rubix-core-*-tests.jar`
  cp ${RUBIX_CORE_TEST_JAR} ${SCRIPT_DIR}/docker/jars/rubix-core_tests.jar
}

start-cluster() {
  SCRIPT_DIR=$(dirname "$0")

  rm -f ${SCRIPT_DIR}/docker/jars/*
  mkdir -p ${SCRIPT_DIR}/docker/jars

  copy-jars-for-containers ${SCRIPT_DIR}

  export DATADIR=$1
  docker-compose -f ${SCRIPT_DIR}/docker/docker-compose.yml up -d
}

stop-cluster() {
  SCRIPT_DIR=$(dirname "$0")

  export DATADIR=$1
  docker-compose -f ${SCRIPT_DIR}/docker/docker-compose.yml down -t 1
}

start-bks() {
  BKS_OPTIONS=$@
  set-cache-options ${BKS_OPTIONS}

  mkdir -p ${RUN_DIR}
  mkdir -p ${LOG_DIR}
  chmod -R 777 ${LOG_DIR}

  setup-disks
  setup-log4j

  bookkeeper_jars=( ${HADOOP_JAR_DIR}/rubix-bookkeeper-*.jar )
  bookkeeper_jar=${bookkeeper_jars[0]}

  export HADOOP_OPTS="-Dlog4j.configuration=file://${LOG4J_FILE}"
  nohup ${HADOOP_DIR}/bin/hadoop jar ${bookkeeper_jar} com.qubole.rubix.bookkeeper.BookKeeperServer ${BKS_OPTIONS} > ${LOG_DIR}/cbk.log 2>&1 &
  echo "$!" > ${PID_FILE}
  echo "Starting Cache BookKeeper server with pid `cat ${PID_FILE}`"

  sleep 1
}

stop-bks() {
  BKS_OPTIONS=$@
  set-cache-options ${BKS_OPTIONS}

  PID=`cat ${PID_FILE}`
  kill -9 ${PID}
  rm -f ${PID_FILE}

  PREFIX=${CACHE_DIR_PREFIX_VALUE}
  SUFFIX=${CACHE_DIR_SUFFIX_VALUE}
  MAX_DISKS=${CACHE_DIR_MAX_DISKS_VALUE}

  for i in $(seq 0 $((MAX_DISKS-1)))
  do
    rm -rf ${PREFIX}${i}${SUFFIX}
  done
}

cmd=$1
case "$cmd" in
  start-bks) shift ; start-bks $@;;
  stop-bks) shift ; stop-bks $@;;
  start-cluster) shift ; start-cluster $@;;
  stop-cluster) shift ; stop-cluster $@;;
esac

exit 0;
