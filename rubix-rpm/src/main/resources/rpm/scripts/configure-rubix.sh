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
RUBIX_SITE="/etc/rubix/rubix-site.xml"
(cat <<CLIENT
<?xml version="1.0"?>
<configuration>
    <property>
        <name>hadoop.cache.data.dirprefix.list</name>
        <value>${RUBIX_CACHE_DIR}/data</value>
    </property>

    <!-- Disable internal caching for FileSystem objects -->
    <property>
        <name>fs.s3.impl.disable.cache</name>
        <value>true</value>
    </property>
    <property>
        <name>fs.s3a.impl.disable.cache</name>
        <value>true</value>
    </property>
    <property>
        <name>fs.s3n.impl.disable.cache</name>
        <value>true</value>
    </property>
    <property>
        <name>fs.wasb.impl.disable.cache</name>
        <value>true</value>
    </property>
</configuration>
CLIENT
) > ${RUBIX_SITE}

MASTER_HOSTNAME=$(grep --no-group-separator -a2 "yarn.resourcemanager.hostname" /usr/lib/hadoop/etc/hadoop/yarn-site.xml | sed  "s/yarn.resourcemanager.hostname/master.hostname/g" | tr -d ' ' | tr -d '\n')
sed -i '$i'${MASTER_HOSTNAME}'' ${RUBIX_SITE}
