#!/bin/bash
#configure bookkeeper server.
INPUT_FILE="/usr/lib/hadoop/etc/hadoop/core-site.xml"
sed -i '$ i <property>\n<name>hadoop.cache.data.dirprefix.list</name>\n<value>/var/lib/rubix/cache/data</value>\n</property>' $INPUT_FILE
sed -i '$ i <property>\n<name>caching.fs.presto-server-port</name>\n<value>8889</value>\n</property>' $INPUT_FILE

host_name=$(grep --no-group-separator -a2 "yarn.resourcemanager.hostname" /usr/lib/hadoop/etc/hadoop/yarn-site.xml | sed  "s/yarn.resourcemanager.hostname/master.hostname/g" | tr -d ' ' | tr -d '\n')
sed -i '$i'$host_name'' $INPUT_FILE

#configure client
OUTPUT_FILE="/etc/rubix/config.xml"
(cat <<EOF
<?xml version="1.0"?>
<configuration>
<property>
<name>hadoop.cache.data.dirprefix.list</name>
<value>/var/lib/rubix/cache/data</value>
</property>
<property>
  <name>fs.rubix.impl</name>
  <value>com.qubole.rubix.presto.CachingPrestoS3FileSystem</value>
  <description>http://wiki.apache.org/hadoop/Hive/HBaseIntegration</description>
</property>
<property>
  <name>caching.fs.presto-server-port</name>
  <value>8889</value>
</property>

EOF
  ) > $OUTPUT_FILE

grep --no-group-separator -a2 "yarn.resourcemanager.address" /usr/lib/hadoop/etc/hadoop/yarn-site.xml >> $OUTPUT_FILE
echo "</configuration>" >> $OUTPUT_FILE

#Configuring Presto Server to include /rubix/config.xml as resource
PRESTO_JVM_CONFIG_FILE="/etc/presto/conf/jvm.config"
config_string="\-Dhive\.config\.resources\="
rubix_config_path=",\/etc\/rubix\/config.xml"
sed -i "/^$config_string/s/$/$rubix_config_path/" $PRESTO_JVM_CONFIG_FILE
