#!/bin/bash

RUBIX_HOME_DIR=/usr/lib/rubix
RUBIX_CACHE_DIR=/var/lib/rubix/cache
RUBIX_MOUNT_DIR=/mnt/rubix

cp -a ${RUBIX_HOME_DIR}/lib/* /usr/lib/hadoop/lib/

# Create & link cache directories
mkdir -p ${RUBIX_MOUNT_DIR}
mkdir -p ${RUBIX_CACHE_DIR}

# disable abort if this symlinking fails
set +e
ln -s ${RUBIX_CACHE_DIR} ${RUBIX_MOUNT_DIR}
set -e

for i in {0..4}
do
    mkdir -p ${RUBIX_CACHE_DIR}/data${i}
done

# Configure BookKeeper server.
HADOOP_CORE_SITE="/usr/lib/hadoop/etc/hadoop/core-site.xml"
sed -i "$ i <property>\n<name>hadoop.cache.data.dirprefix.list</name>\n<value>${RUBIX_CACHE_DIR}/data</value>\n</property>" ${HADOOP_CORE_SITE}

MASTER_HOSTNAME=$(grep --no-group-separator -a2 "yarn.resourcemanager.hostname" /usr/lib/hadoop/etc/hadoop/yarn-site.xml | sed  "s/yarn.resourcemanager.hostname/master.hostname/g" | tr -d ' ' | tr -d '\n')
sed -i '$i'${MASTER_HOSTNAME}'' ${HADOOP_CORE_SITE}
