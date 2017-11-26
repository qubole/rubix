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

## Start RubiX Daemons
    rubix_admin daemon start --debug
    # To verfiy the daemons are up
       verfiy process ids for both 
       BookKeeperServer and LocalDiscoveryServer.
    sudo jps -m
    
## Restart Presto Server
    sudo restart presto-server
    
## Create Table Using Hive

### Start Hive Client

    hive --hiveconf hive.metastore.uris="" --hiveconf fs.rubix.impl=com.qubole.rubix.hadoop2.CachingNativeS3FileSystem

### Create External Table

    CREATE EXTERNAL TABLE wikistats_orc_rubix 
    (language STRING, page_title STRING,
    hits BIGINT, retrived_size BIGINT)
    STORED AS ORC
    LOCATION 'rubix://emr.presto.airpal/wikistats/orc';

## Start Presto-cli
    presto-cli --catalog hive --schema default

## Query Execution

Run the following query

    SELECT language, page_title, AVG(hits) AS avg_hits
    FROM default.wikistats_orc_rubix
    WHERE language = 'en'
    AND page_title NOT IN ('Main_Page',  '404_error/')
    AND page_title NOT LIKE '%index%'
    AND page_title NOT LIKE '%Search%'
    GROUP BY language, page_title
    ORDER BY avg_hits DESC
    LIMIT 10;

## Rubix Stats

The cache statistics are pushed to MBean named rubix:name=stats. To check the stats, execute

    SELECT Node, CachedReads, 
    ROUND(extrareadfromremote,2) as ExtraReadFromRemote, 
    ROUND(hitrate,2) as HitRate, 
    ROUND(missrate,2) as MissRate,  
    ROUND(nonlocaldataread,2) as NonLocalDataRead, 
    NonLocalReads,
    ROUND(readfromcache,2) as ReadFromCache, 
    ROUND(readfromremote, 2) as ReadFromRemote, 
    RemoteReads
    FROM jmx.current."rubix:name=stats";

