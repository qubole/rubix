#!/bin/bash
#configure bookkeeper server.
INPUT_FILE="/usr/lib/hadoop/etc/hadoop/core-site.xml"
sed -i '$ i <property>\n<name>hadoop.cache.data.dirprefix.list</name>\n<value>/var/lib/rubix/cache/data</value>\n</property>' $INPUT_FILE


#configure client
OUTPUT_FILE="/etc/rubix/config.xml"
(cat <<EOF
<?xml version="1.0"?>
<configuration>
<property>
<name>hadoop.cache.data.dirprefix.list</name>
<value>/var/lib/rubix/cache/data</value>
</property>
EOF
  ) > $OUTPUT_FILE

grep --no-group-separator -a2 "yarn.resourcemanager.address" /usr/lib/hadoop/etc/hadoop/yarn-site.xml >> $OUTPUT_FILE
echo "</configuration>" >> $OUTPUT_FILE