FROM phusion/baseimage:0.11

#######################
### BASEIMAGE SETUP ###
#######################

CMD ["/sbin/my_init"]

####################
### CUSTOM SETUP ###
####################


ARG is_master
ENV IS_CLUSTER_MASTER=$is_master

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

# Copy RubiX JARs
COPY jars/rubix-bookkeeper-*.jar /usr/lib/rubix/lib/rubix-bookkeeper.jar
COPY jars/rubix-common-*.jar /usr/lib/rubix/lib/rubix-common.jar
COPY jars/rubix-core-*.jar /usr/lib/rubix/lib/rubix-core.jar
COPY jars/rubix-core_tests.jar /usr/lib/rubix/lib/rubix-core-tests.jar
COPY jars/rubix-spi-*.jar /usr/lib/rubix/lib/rubix-spi.jar
COPY jars/rubix-hadoop2-*.jar /usr/lib/rubix/lib/rubix-hadoop2.jar
COPY jars/rubix-client-*.jar /usr/lib/rubix/lib/rubix-client.jar

# Setup logging
RUN mkdir /home/logs/
COPY logging/log4j_bks.properties \
    logging/log4j_lds.properties \
    logging/log4j_crs.properties \
    /home/props/

##########################
### CONTAINER REQUESTS ###
##########################

RUN mkdir /etc/service/crs
COPY start-crs.sh /etc/service/crs/run
RUN chmod +x /etc/service/crs/run

##################
### BOOKKEEPER ###
##################

RUN mkdir /etc/service/bks
COPY start-bks.sh /etc/service/bks/run
RUN chmod +x /etc/service/bks/run

###########################
### LOCAL DATA TRANSFER ###
###########################

RUN mkdir /etc/service/lds
COPY start-lds.sh /etc/service/lds/run
RUN chmod +x /etc/service/lds/run
