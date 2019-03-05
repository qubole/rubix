#!/bin/bash -e

mvn clean install -DskipTests -Dcheckstyle.skip=true

PWD=$(pwd)
HADOOP_JARSPATH="/usr/lib/hadoop2/share/hadoop/tools/lib/"

RUBIX_JARS=`ls ${PWD}/rubix-*/target/rubix-*.jar | grep -v tests`

sudo cp $RUBIX_JARS $HADOOP_JARSPATH

mvn clean install -Dcheckstyle.skip=true
if [[ $? -ne 0 ]]; then
    exit 1
fi
mvn cobertura:cobertura-integration-test
bash <(curl -s https://codecov.io/bash)
