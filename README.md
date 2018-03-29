# RubiX

[![Build Status](https://travis-ci.org/qubole/rubix.svg?branch=master)](https://travis-ci.org/qubole/rubix)
[![Coverage Status](https://coveralls.io/repos/github/qubole/rubix/badge.svg)](https://coveralls.io/github/qubole/rubix)

RubiX is a light-weight data caching framework that can be used by Big-Data engines.
RubiX can be extended to support any engine that accesses data in cloud stores using Hadoop FileSystem interface via plugins. 
Using the same plugins, RubiX can also be extended to be used with any cloud store

### Usecase

RubiX provides disk or in-memory caching of data, which would otherwise be accessed over network when it resides in cloud store,
thereby improving performance.

### Supported Engines and Cloud Stores

- Presto: Amazon S3  
- Spark: Amazon S3  
- Any engine using hadoop-2 or hadoop-1, e.g. Hive can utilize RubiX. Amazon S3 is supported  

###  How to use it

RubiX has two components: a BookKeeper server and a FileSystem implementation that an engine should use.

List of things to be done to use RubiX are:

1. Start the BookKeeper server. It can be started via `hadoop jar` command, e.g.:   
>	hadoop jar rubix-bookkeeper-${rubix-version}.jar com.qubole.rubix.bookkeeper.BookKeeperServer  
>	hadoop jar rubix-bookkeeper-${rubix-version}.jar com.qubole.rubix.bookkeeper.LocalDataTransferServer

2. Engine side changes:   
	To use RubiX, you need to place the appropriate jars in the classpath and configure Engines to use RubiX filesystem to access the cloud store. Sections below show how to get started on RubiX with supported plugins

##### Using RubiX with Presto
  
1. Place rubix-bookkeeper.jar, rubix-core.jar, rubix-presto.jar in presto/plugin/hive-hadoop2/ directory.   
   All these jars are packaged in rubix-presto.tar under assembly module
2. Configuration changes    
     i. Set configuration to use RubiX filesystem in Presto.   
     ii. Set "hive.force-local-scheduling=true" in hive.properties   
3. Start/Re-start the Presto server
		
##### Using RubiX with Hive

1. Add RubiX jars:  rubix-bookkeeper.jar, rubix-core.jar, rubix-hadoop1 either to hadoop/lib directly or via `add jar` command.   
	    All these jars are packaged in rubix-hadoop1.tar under assembly module   
2. Configuration changes: Use following configs to start using RubiX   
		fs.s3n.impl=com.qubole.rubix.hadoop1.CachingNativeS3FileSystem   
		fs.s3.impl=com.qubole.rubix.hadoop1.CachingNativeS3FileSystem   

##### Using Rubix with Spark  

1. Add the Rubix jars to Driver and Executor classpaths and use the rubix implementation for hadoop.fs.s3 and hadoop.fs.s3n.  
	--conf spark.hadoop.fs.s3.impl=com.qubole.rubix.hadoop2.CachingNativeS3FileSystem  
	--conf spark.hadoop.fs.s3n.impl=com.qubole.rubix.hadoop2.CachingNativeS3FileSystem  
	--conf spark.driver.extraClassPath=/usr/lib/hadoop2/share/hadoop/common/lib/rubix-core-${rubix-version}.jar,/usr/lib/hadoop2/share/hadoop/common/lib/rubix-hadoop2-${rubix-version}.jar,/usr/lib/hadoop2/share/hadoop/common/lib/rubix-bookkeeper-${rubix-version}.jar  
	--conf spark.executor.extraClassPath=/usr/lib/hadoop2/share/hadoop/common/lib/rubix-core-${rubix-version}.jar,/usr/lib/hadoop2/share/hadoop/common/lib/rubix-hadoop2-${rubix-version}.jar,/usr/lib/hadoop2/share/hadoop/common/lib/rubix-bookkeeper-${rubix-version}.jar  
2. Start/Re-start the Spark Application.  



### Configurations

##### BookKeeper server configurations
These configurations are to be providing as hadoop configs while started the BookKeeper server

| Configuration                            | Default          | Description                                                                                                                                                                    |
|------------------------------------------|------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| hadoop.cache.data.bookkeeper.port        | 8899             | The port on which BookKeeper server will listen                                                                                                                                |
| hadoop.cache.data.bookkeeper.max-threads | unbounded        | Maximum number of threads BookKeeper can launch                                                                                                                                |
| hadoop.cache.data.block-size             | 1048576          | The size in bytes in which the file is logically divided internally. Higher value means lesser space requirement for metadata but can cause reading of more additional data than needed |
| hadoop.cache.data.dirprefix.list         | /media/ephemeral | Prefixes for paths of directories used to store cached data. Final paths created by appending suffix in range [0, 5] followed by fcache.                                       |
| hadoop.cache.data.fullness.percentage    | 80               | Percentage of total disk space to use for caching and backing files are deleted in an LRU way.                                                                                 |
| hadoop.cache.data.expiration             | unbounded        | How long data is kept in cache                                                                                                                                                 |
##### FileSystem configurations
These configurations need to be provided by the engine which is going to use RubiX

| Configuration                        | Default | Description                                                                                                                                                                                                               |
|--------------------------------------|---------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| hadoop.cache.data.enabled            | true    | Control using cache or not                                                                                                                                                                                                |
| hadoop.cache.data.strict.mode        | false   | By default RubiX tries not to fail read requests if there are some errors and tries to fallback to reading directly from remote source. Setting this config to true will fail read request if there were errors in RubiX. |
| hadoop.cache.data.location.blacklist | empty   | Regex blacklisting locations that should not be cached                                                                                                                                                                    |

### Monitoring

Client side monitoring is set up right  now, stats are published to MBean named `rubix:name=stats`

Engines which provide interface to view jmx stats can see these stats. E.g. in Presto you can run this query to see the stats:
>
```
presto:default> select * from jmx.jmx."rubix:name=stats";
    node     | cachedreads | extrareadfromremote |      hitrate       |      missrate      |   readfromcache    |  readfromremote   | remotereads | warmuppenalty 
-------------+-------------+---------------------+--------------------+--------------------+--------------------+-------------------+-------------+---------------
 presto-vbox |          25 |  1.7881784439086914 | 0.5681818181818182 | 0.4318181818181818 | 18.379225730895996 | 18.55983543395996 |          19 |             0 
(1 row)
```

### Building

#### Setup Thrift
Note that you will need thrift-0.9.3 installed

    # Mac OS X/Brew instructions
    
    ## Installs thrift 0.10.0
    brew install thrift
    
    ### Install thrift 0.9.3
    brew unlink thrift
    brew install https://raw.githubusercontent.com/Homebrew/homebrew-core/9d524e4850651cfedd64bc0740f1379b533f607d/Formula/thrift.rb

#### Compile and install
    mvn clean install

#### Generate RPM
    mvn clean package -Prpm,default
    
    ## rpm and rpmbuild is required
    ## To install rpm on Mac OS X
    brew install rpm
    
RPM is created in _rubix-rpm/target/rpm/qubole-rubix/RPMS/noarch/_    
### Need Help?
You can post your queries to https://groups.google.com/forum/#!forum/rubix-users
