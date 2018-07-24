FROM jamesdbloom/docker-java7-maven
MAINTAINER Kamesh Vankayala - Qubole Inc.

## Install thrift 0.9.3
ENV THRIFT_VERSION 0.9.3

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


RUN gpg --keyserver pool.sks-keyservers.net --recv-keys \
    07617D4968B34D8F13D56E20BE5AAA0BA210C095 \
    2CAC83124870D88586166115220F69801F27E622 \
    4B96409A098DBD511DF2BC18DBAF69BEA7239D59 \
    9DD955653083EFED6171256408458C39E964B5FF \
    B6B3F7EDA5BA7D1E827DE5180DFF492D8EE2F25C \
    6A67379BEFC1AE4D5595770A34005598B8F47547 \
    47660BC98BC433F01E5C90581209E7F13D0C92B9 \
    CE83449FDC6DACF9D24174DCD1F99F6EE3CD2163 \
    A11DF05DEA40DA19CE4B43C01214CF3F852ADB85 \
    686E5EDF04A4830554160910DF0F5BBC30CD0996 \
    5BAE7CB144D05AD1BB1C47C75C6CC6EFABE49180 \
    AF7610D2E378B33AB026D7574FB955854318F669 \
    6AE70A2A38F466A5D683F939255ADF56C36C5F0F \
    70F7AB3B62257ABFBD0618D79FDB12767CC7352A \
    842AAB2D0BC5415B4E19D429A342433A56D8D31A \
    1B5D384B734F368052862EB55E43CAB9AEC77EAF \
    785436A782586B71829C67A04169AA27ECB31663 \
    5E49DA09E2EC9950733A4FF48F1895E97869A2FB \
    A13B3869454536F1852C17D0477E02D33DD51430 \
    A6220FFCC86FE81CE5AAC880E3814B59E4E11856 \
    EFE2E7C571309FE00BEBA78D5E314EEF7340E1CB \
    EB34498A9261F343F09F60E0A9510905F0B000F0 \
    3442A6594268AC7B88F5C1D25104A731B021B57F \
    6E83C32562C909D289E6C3D98B25B9B71EFF7770 \
    E9216532BF11728C86A11E3132CF4BF4E72E74D3 \
    E8966520DA24E9642E119A5F13971DA39475BD5D \
    1D369094D4CFAC140E0EF05E992230B1EB8C6EFA \
    A312CE6A1FA98892CB2C44EBA79AB712DE5868E6 \
    0445B7BFC4515847C157ECD16BA72FF1C99785DE \
    B74F188889D159F3D7E64A7F348C6D7A0DCED714 \
    4A6AC5C675B6155682729C9E08D51A0A7501105C \
    8B44A05C308955D191956559A5CEE20A90348D47

ENV HADOOP_VERSION 2.7.2
ENV HADOOP_URL https://archive.apache.org/dist/hadoop/core/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz

RUN set -x \
    && curl -fSL "$HADOOP_URL" -o /tmp/hadoop.tar.gz \
    && curl -fSL "$HADOOP_URL.asc" -o /tmp/hadoop.tar.gz.asc \
    && gpg --verify /tmp/hadoop.tar.gz.asc \
    && tar -xvf /tmp/hadoop.tar.gz -C /usr/lib/ \
    && rm /tmp/hadoop.tar.gz*

ENV HADOOP_PREFIX /usr/lib/hadoop

RUN cd /usr/lib && mv hadoop-$HADOOP_VERSION hadoop2 && ln -s ./hadoop2 hadoop

RUN mkdir $HADOOP_PREFIX/input
RUN cp $HADOOP_PREFIX/etc/hadoop/*.xml $HADOOP_PREFIX/input

ENV HADOOP_CLASSPATH=$HADOOP_CLASSPATH:/usr/lib/hadoop2/share/hadoop/tools/lib/*

ENV HADOOP_CONF_DIR=/etc/hadoop
ENV MULTIHOMED_NETWORK=1

ENV USER=root
ENV PATH $HADOOP_PREFIX/bin/:$PATH

RUN mkdir -p /media/ephemeral0
