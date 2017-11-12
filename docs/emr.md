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

    pip install rubix_admin

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


## Install RubiX
    rubix_admin installer install --rpm <path-to-rubix-rpm> 

To enable debugging and see the rubix activity, create /usr/lib/presto/etc/log.properties file with bellow config.
com.qubole=DEBUG

## Start rubix-daemons
    rubix_admin daemon start --debug
    # To verfiy the daemons are up
       verfiy process ids for both 
       BookKeeperServer and LocalDiscoveryServer.
    sudo jps -m
    
## Restart Presto Server
    sudo restart presto-server
    
## Start the presto cli
    presto-cli --catalog hive --schema default
