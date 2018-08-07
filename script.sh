#!/bin/bash -e

mvn clean install -DskipTests

PWD=$(pwd)
HADOOP_JARSPATH="/usr/lib/hadoop2/share/hadoop/tools/lib/"

RUBIX_SPI_JAR="${PWD}/rubix-spi/target/rubix-spi-*SNAPSHOT.jar"
RUBIX_CORE_JAR="${PWD}/rubix-core/target/rubix-core-*SNAPSHOT.jar"
RUBIX_PRESTO_JAR="${PWD}/rubix-presto/target/rubix-presto-*SNAPSHOT.jar"
RUBIX_HADOOP2_JAR="${PWD}/rubix-hadoop2/target/rubix-hadoop2-*SNAPSHOT.jar"
RUBIX_BOOKKEEPER_JAR="${PWD}/rubix-bookkeeper/target/rubix-bookkeeper-*SNAPSHOT.jar"

sudo cp $RUBIX_HADOOP2_JAR $HADOOP_JARSPATH
sudo cp $RUBIX_BOOKKEEPER_JAR $HADOOP_JARSPATH
sudo cp $RUBIX_CORE_JAR $HADOOP_JARSPATH
sudo cp $RUBIX_PRESTO_JAR $HADOOP_JARSPATH
sudo cp $RUBIX_SPI_JAR $HADOOP_JARSPATH

mvn clean install
if [[ $? -ne 0 ]]; then
    exit 1
fi
mvn cobertura:cobertura
bash <(curl -s https://codecov.io/bash)
