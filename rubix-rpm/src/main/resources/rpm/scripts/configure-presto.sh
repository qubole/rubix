#!/bin/bash

RUBIX_HOME_DIR=/usr/lib/rubix
RUBIX_CACHE_DIR=/var/lib/rubix/cache
PRESTO_HOME_DIR=/usr/lib/presto
HADOOP_HOME_DIR=/usr/lib/hadoop

# Configure BookKeeper server for PrestoClusterManager
HADOOP_CORE_SITE=${HADOOP_HOME_DIR}/etc/hadoop/core-site.xml
sed -i '$ i <property>\n<name>caching.fs.presto-server-port</name>\n<value>8889</value>\n</property>' ${HADOOP_CORE_SITE}

# Configure Presto as RubiX client
RUBIX_PRESTO_CLIENT_CONFIG="/etc/rubix/presto-config.xml"
(cat <<CLIENT
<?xml version="1.0"?>
<configuration>
    <property>
        <name>hadoop.cache.data.dirprefix.list</name>
        <value>${RUBIX_CACHE_DIR}/data</value>
    </property>
    <property>
        <name>fs.rubix.impl</name>
        <value>com.qubole.rubix.presto.CachingPrestoS3FileSystem</value>
    </property>
    <property>
        <name>caching.fs.presto-server-port</name>
        <value>8889</value>
    </property>

CLIENT
) > ${RUBIX_PRESTO_CLIENT_CONFIG}

grep --no-group-separator -a2 "yarn.resourcemanager.address" ${HADOOP_HOME_DIR}/etc/hadoop/yarn-site.xml >> ${RUBIX_PRESTO_CLIENT_CONFIG}
echo "</configuration>" >> ${RUBIX_PRESTO_CLIENT_CONFIG}

# Configure Presto Server to include RubiX client configuration as resource
PRESTO_HIVE_CONFIG=/etc/presto/conf/catalog/hive.properties
HIVE_RESOURCES="hive\.config\.resources"
sed -i "/^${HIVE_RESOURCES}/ s|$|,${RUBIX_PRESTO_CLIENT_CONFIG}|" ${PRESTO_HIVE_CONFIG}

# Copy RubiX JARs to Presto plugin directory
cp -a ${RUBIX_HOME_DIR}/lib/* ${PRESTO_HOME_DIR}/plugin/hive-hadoop2/

restart presto-server
