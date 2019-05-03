FROM openjdk:7-jdk-alpine

# Download & install Hadoop
ENV HADOOP_VERSION 2.6.0
ENV HADOOP_URL https://archive.apache.org/dist/hadoop/core/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz
ENV HADOOP_HOME /usr/lib/hadoop
ENV HADOOP_CLASSPATH /usr/lib/rubix/lib/*
ENV PATH $JAVA_HOME/bin:$PATH
ENV HADOOP_OPTS "-Dlog4j.configuration=file:///tmp/log4j_bks.properties"
# Get build deps
RUN apk update && apk add curl bash \
    && set -x && curl -fSL "$HADOOP_URL" -o /tmp/hadoop.tar.gz \
    && tar -xf /tmp/hadoop.tar.gz -C /usr/lib/ \
    && rm /tmp/hadoop.tar.gz* \
    && cd /usr/lib && mv hadoop-$HADOOP_VERSION hadoop2 && ln -s ./hadoop2 hadoop \
    && mkdir -p /media/ephemeral0

COPY jars/rubix-bookkeeper-*.jar /usr/lib/rubix/lib/rubix-bookkeeper.jar
COPY jars/rubix-common-*.jar /usr/lib/rubix/lib/rubix-common.jar
COPY jars/rubix-core-*.jar /usr/lib/rubix/lib/rubix-core.jar
COPY jars/rubix-spi-*.jar /usr/lib/rubix/lib/rubix-spi.jar
COPY jars/rubix-hadoop2-*.jar /usr/lib/rubix/lib/rubix-hadoop2.jar

COPY start-servers.sh /tmp/

COPY logging/log4j_bks.properties logging/log4j_lds.properties /tmp/
