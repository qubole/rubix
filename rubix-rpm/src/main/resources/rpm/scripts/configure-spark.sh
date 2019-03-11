#!/bin/bash

RUBIX_HOME_DIR=/usr/lib/rubix

# Configure Spark as RubiX client
SPARK_DEFAULTS_CONF_FILE="${SPARK_HOME}/conf/spark-defaults.conf"
SPARK_DRIVER_EXTRA_CP_OPTION="spark.driver.extraClassPath"
SPARK_EXECUTOR_EXTRA_CP_OPTION="spark.executor.extraClassPath"
RUBIX_EXTRA_CP="${RUBIX_HOME_DIR}/lib/*"

if grep -qF ${SPARK_DRIVER_EXTRA_CP_OPTION} "${SPARK_DEFAULTS_CONF_FILE}"; then
    DRIVER_OPTION_REGEX=$(echo ${SPARK_DRIVER_EXTRA_CP_OPTION} | sed 's|\.|\\\.|')
    sed -i "/^$DRIVER_OPTION_REGEX/ s|$|:$RUBIX_EXTRA_CP|" ${SPARK_DEFAULTS_CONF_FILE}
else
    echo "${SPARK_DRIVER_EXTRA_CP_OPTION}    ${RUBIX_EXTRA_CP}" >> ${SPARK_DEFAULTS_CONF_FILE}
fi

if grep -qF ${SPARK_EXECUTOR_EXTRA_CP_OPTION} "${SPARK_DEFAULTS_CONF_FILE}"; then
    EXECUTOR_OPTION_REGEX=$(echo ${SPARK_EXECUTOR_EXTRA_CP_OPTION} | sed 's|\.|\\\.|')
    sed -i "/^$EXECUTOR_OPTION_REGEX/ s|$|:$RUBIX_EXTRA_CP|" ${SPARK_DEFAULTS_CONF_FILE}
else
    echo "${SPARK_EXECUTOR_EXTRA_CP_OPTION}    ${RUBIX_EXTRA_CP}" >> ${SPARK_DEFAULTS_CONF_FILE}
fi

echo "spark.hadoop.fs.s3.impl   com.qubole.rubix.hadoop2.CachingNativeS3FileSystem" >> ${SPARK_DEFAULTS_CONF_FILE}
echo "spark.hadoop.fs.s3n.impl  com.qubole.rubix.hadoop2.CachingNativeS3FileSystem" >> ${SPARK_DEFAULTS_CONF_FILE}
echo "spark.hadoop.fs.s3a.impl  com.qubole.rubix.hadoop2.CachingS3AFileSystem" >> ${SPARK_DEFAULTS_CONF_FILE}
echo "spark.hadoop.fs.wasb.impl com.qubole.rubix.hadoop2.CachingNativeAzureFileSystem" >> ${SPARK_DEFAULTS_CONF_FILE}
echo "spark.hadoop.hadoop.cache.data.dirprefix.list     /var/lib/rubix/cache/data" >> ${SPARK_DEFAULTS_CONF_FILE}
