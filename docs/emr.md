# Installation Guide

## Start an EMR Cluster (Optional)

Create EMR cluster, use 'advanced option' to create a cluster with the required spec.
a) Specify atleast 30GB space for Root device EBS volume.
b) To login into cluster, choose a EC2 key pair.

## Connect to Master Node

Log into master and all slave nodes as "hadoop" user.

    ssh <key> hadoop@master-public-ip and install required libs.

Setup and check passwordless SSH between cluster machines

    ssh hadoop@localhost
    ssh hadoop@worker-public-ip

## Install RubiX Admin

    pip install rubix_admin

## Update Config File

    rubix_admin -h

This will create rubix-admin config file at ~/.radminrc with the follwoing format

    hosts:
      - localhost
      - worker-ip1
      - worker-ip2
      ..
    remote_packages_path: /tmp/rubix_rpms

## Install RubiX
    rubix_admin installer install

This will install the latest version of RubiX. To install a specific version of Rubix,

    rubix_admin installer install --rpm-version <RubiX Version>

To install from a rpm file,

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
