# Installation Guide

## Start an EMR Cluster (Optional)

Create EMR cluster, use 'advanced option' to create a cluster with the required spec.
a) presto should be unchecked, we take a vanila hadoop-hive distribution and install qubole presto on top of it.
b) Specify atleast 30GB space for Root device EBS volume.
c) To login into cluster, choose a EC2 key pair.

## Install Software on Master Node

Log into master and all slave nodes as "hadoop" user.

    ssh <key> hadoop@master-public-ip and install required libs.

### JAVA8
    wget --no-cookies --header "Cookie: gpw_e24=xxx; oraclelicense=accept-securebackup-cookie;" "http://download.oracle.com/otn-pub/java/jdk/8u131-b11/d54c1d3a095b4ff2b6607d096fa80163/jdk-8u131-linux-x64.rpm" 
    sudo rpm -i jdk-8u131-linux-x64.rpm

    # Update JAVA_HOME in ~/.bashrc

### PYTHON26

    sudo yum install python26


## Install RubiX Admin

    pip install rubix-admin

## Setup and check passwordless SSH between cluster machines
   
    ssh hadoop@localhost
    ssh hadoop@worker-public-ip

Create rubix-admin config file at ~/.radminrc, bellow is the sample format.

    hosts:
      - localhost
      - worker-ip1
      - worker-ip2 
      ..
    remote_packages_path: /tmp/rubix_rpms


## Install Qubole Presto & RubiX
    hadoop dfs -get s3://public-qubole/presto/presto-server-rpm-0.180-q1-SNAPSHOT.x86_64.rpm
    rubix-admin installer install --rpm <path-to-qubole-presto-rpm> <path-to-rubix-rpm> 

## Configure Presto

After the installation has completed, update config.properties of *all* the nodes.
Update /usr/lib/presto/etc/config.properties with the following values,

### Master
    coordinator=true
    node-scheduler.include-coordinator=false
    http-server.http.port=8081
    query.max-memory=10GB
    query.max-memory-per-node=1GB
    discovery-server.enabled=true
    discovery.uri=localhost:8081

### Workers
    coordinator=false
    http-server.http.port=8081
    query.max-memory=10GB
    query.max-memory-per-node=1GB
    discovery.uri=http://<master-public-ip>:8081

To enable debugging and see the rubix activity, create /usr/lib/presto/etc/log.properties file with bellow config.
com.qubole=DEBUG

## Start rubix-daemons & Qubole Presto
    rubix-admin daemon start --debug
    # To verfiy the daemons are up
       verfiy process ids for both 
       BookKeeperServer and LocalDiscoveryServer.
    sudo jps -m
    
    # start the presto cluster on each node with 
    /usr/lib/presto/bin/launcher
