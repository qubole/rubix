FROM phusion/baseimage:0.11

#######################
### BASEIMAGE SETUP ###
#######################

ENV HADOOP_VERSION 2.6.0
ENV HADOOP_URL https://archive.apache.org/dist/hadoop/core/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz
ENV HADOOP_HOME /usr/lib/hadoop
ENV HADOOP_CLASSPATH /usr/lib/rubix/lib/*
ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64
ENV PATH $JAVA_HOME/bin:$PATH

# Get build/run deps
RUN apt-get update && apt-get -y install openjdk-8-jdk

# Download & install Hadoop
RUN set -x && curl -fSL "$HADOOP_URL" -o /tmp/hadoop.tar.gz \
    && tar -xf /tmp/hadoop.tar.gz -C /usr/lib/ \
    && rm /tmp/hadoop.tar.gz* \
    && cd /usr/lib && mv hadoop-$HADOOP_VERSION hadoop2 && ln -s ./hadoop2 hadoop \
    && mkdir -p /media/ephemeral0
