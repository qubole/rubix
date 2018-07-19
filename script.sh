#!/bin/bash

mvn clean install -DskipTests

PWD=$(pwd)
echo $PWD
HADOOP_JARSPATH="/usr/lib/hadoop2/share/hadoop/tools/lib/"

RUBIX_SPI_JAR="${PWD}/rubix-spi/target/rubix-spi-*.jar"
RUBIX_CORE_JAR="${PWD}/rubix-core/target/rubix-core-*SNAPSHOT.jar"
RUBIX_PRESTO_JAR="${PWD}/rubix-presto/target/rubix-presto-*.jar"
RUBIX_HADOOP2_JAR="${PWD}/rubix-hadoop2/target/rubix-hadoop2-*.jar"
RUBIX_BOOKKEEPER_JAR="${PWD}/rubix-bookkeeper/target/rubix-bookkeeper-*.jar"

echo $RUBIX_SPI_JAR
echo $RUBIX_CORE_JAR
echo $RUBIX_PRESTO_JAR
echo $RUBIX_HADOOP2_JAR
echo $RUBIX_BOOKKEEPER_JAR

sudo cp $RUBIX_HADOOP2_JAR $HADOOP_JARSPATH
sudo cp $RUBIX_BOOKKEEPER_JAR $HADOOP_JARSPATH
sudo cp $RUBIX_CORE_JAR $HADOOP_JARSPATH
sudo cp $RUBIX_PRESTO_JAR $HADOOP_JARSPATH
sudo cp $RUBIX_SPI_JAR $HADOOP_JARSPATH

mvn clean install
