FROM jamesdbloom/docker-java7-maven
MAINTAINER Kamesh Vankayala - Qubole Inc.

## Install thrift 0.9.3
ENV THRIFT_VERSION 0.9.3

RUN printf "deb http://archive.debian.org/debian/ jessie main\ndeb-src http://archive.debian.org/debian/ jessie main\ndeb http://security.debian.org jessie/updates main\ndeb-src http://security.debian.org jessie/updates main" > /etc/apt/sources.list

RUN buildDeps=" \
		automake \
		bison \
		curl \
		flex \
		g++ \
		libboost-dev \
		libboost-filesystem-dev \
		libboost-program-options-dev \
		libboost-system-dev \
		libboost-test-dev \
		libevent-dev \
		libssl-dev \
		libtool \
		make \
		pkg-config \
	"; \
	apt-get update && apt-get install -y --no-install-recommends $buildDeps && rm -rf /var/lib/apt/lists/* \
	&& curl -sSL "http://apache.mirrors.spacedump.net/thrift/$THRIFT_VERSION/thrift-$THRIFT_VERSION.tar.gz" -o thrift.tar.gz \
	&& mkdir -p /usr/src/thrift \
	&& tar zxf thrift.tar.gz -C /usr/src/thrift --strip-components=1 \
	&& rm thrift.tar.gz \
	&& cd /usr/src/thrift \
	&& ./configure  --without-python --without-cpp \
	&& make \
	&& make install \
	&& cd / \
	&& rm -rf /usr/src/thrift \
	&& curl -k -sSL "https://storage.googleapis.com/golang/go1.4.linux-amd64.tar.gz" -o go.tar.gz \
	&& tar xzf go.tar.gz \
	&& rm go.tar.gz \
	&& cp go/bin/gofmt /usr/bin/gofmt \
	&& rm -rf go \
	&& apt-get purge -y --auto-remove $buildDeps

## Install git
RUN apt-get -y install git

# install dev tools
RUN apt-get update
RUN apt-get install -y curl tar sudo openssh-server openssh-client rsync

ENV HADOOP_VERSION 2.7.2
ENV HADOOP_URL https://archive.apache.org/dist/hadoop/core/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz

RUN set -x \
    && curl -fSL "$HADOOP_URL" -o /tmp/hadoop.tar.gz \
    && tar -xvf /tmp/hadoop.tar.gz -C /usr/lib/ \
    && rm /tmp/hadoop.tar.gz*

ENV HADOOP_PREFIX /usr/lib/hadoop

RUN cd /usr/lib && mv hadoop-$HADOOP_VERSION hadoop2 && ln -s ./hadoop2 hadoop

ENV HADOOP_CLASSPATH=$HADOOP_CLASSPATH:/usr/lib/hadoop2/share/hadoop/tools/lib/*

ENV USER=root
ENV PATH $HADOOP_PREFIX/bin/:$PATH

# Install Docker and Docker Compose for integration tests
RUN set -x \
    && curl -fSL "https://download.docker.com/linux/static/stable/x86_64/docker-17.09.0-ce.tgz" -o /tmp/docker-ce.tgz \
    && tar -xvzf /tmp/docker-ce.tgz \
    && cp docker/* /usr/local/bin

RUN set -x \
    && curl -L "https://github.com/docker/compose/releases/download/1.24.0/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose \
    && chmod +x /usr/local/bin/docker-compose

RUN mkdir -p /media/ephemeral0

RUN apt-get install -yqq python

RUN apt-get -yqq install python-pip
