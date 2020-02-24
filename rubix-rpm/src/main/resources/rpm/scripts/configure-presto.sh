#!/bin/bash

RUBIX_HOME_DIR=/usr/lib/rubix
RUBIX_CACHE_DIR=/var/lib/rubix/cache

# Configure BookKeeper server for PrestoClusterManager
RUBIX_SITE=/etc/rubix/rubix-site.xml
sed -i '$ i <property>\n<name>caching.fs.presto-server-port</name>\n<value>8889</value>\n</property>' ${RUBIX_SITE}

# Configure Presto as RubiX client
RUBIX_PRESTO_CLIENT_CONFIG="/etc/rubix/presto-config.xml"
(cat <<CLIENT
<?xml version="1.0"?>
<configuration>
    <property>
        <name>rubix.cache.dirprefix.list</name>
        <value>${RUBIX_CACHE_DIR}/data</value>
    </property>
    <property>
        <name>fs.rubix.impl</name>
        <value>com.qubole.rubix.presto.CachingPrestoS3FileSystem</value>
    </property>
    <property>
        <name>fs.wasb.impl</name>
        <value>com.qubole.rubix.presto.CachingPrestoNativeAzureFileSystem</value>
    </property>
    <property>
        <name>fs.abfs.impl</name>
        <value>com.qubole.rubix.presto.CachingPrestoAzureBlobFileSystem</value>
    </property>
    <property>
        <name>fs.abfss.impl</name>
        <value>com.qubole.rubix.presto.CachingPrestoSecureAzureBlobFileSystem</value>
    </property>
    <property>
        <name>caching.fs.presto-server-port</name>
        <value>8889</value>
    </property>

CLIENT
) > ${RUBIX_PRESTO_CLIENT_CONFIG}

grep --no-group-separator -a2 "yarn.resourcemanager.address" ${HADOOP_HOME}/etc/hadoop/yarn-site.xml >> ${RUBIX_PRESTO_CLIENT_CONFIG}
echo "</configuration>" >> ${RUBIX_PRESTO_CLIENT_CONFIG}

# Configure Presto Server to include RubiX client configuration as resource
if [[ -d ${PRESTO_HOME}/etc ]]; then
    # Symlink directory from expected location
    set +e
    ln -s /etc/presto/conf/catalog ${PRESTO_HOME}/etc
    set -e
fi
PRESTO_HIVE_CONFIG=${PRESTO_HOME}/etc/catalog/hive.properties
HIVE_RESOURCES="hive\.config\.resources"
sed -i "/^${HIVE_RESOURCES}/ s|$|,${RUBIX_PRESTO_CLIENT_CONFIG}|" ${PRESTO_HIVE_CONFIG}

# Copy RubiX JARs to Presto plugin directory
cp -a ${RUBIX_HOME_DIR}/lib/* ${PRESTO_HOME}/plugin/hive-hadoop2/

restart presto-server
