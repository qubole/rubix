# RubiX

RubiX is a light-weight data caching framework that can be used by Big-Data engines.
RubiX can be extended to support any engine that accesses data in cloud stores using Hadoop FileSystem interface via plugins. 
Using the same plugins, RubiX can also be extended to be used with any cloud store

### Usecase

RubiX provides disk or in-memory caching of data, which would otherwise be accessed over network when it resides in cloud store,
thereby improving performance.

### Supported Engines and Cloud Stores

- Presto: Amazon S3 is supported.
- Hadoop-1: Any engine using hadoop-1, e.g. Hive can utilize RubiX. Amazon S3 is supported.
- Hadoop-2/Tez: Any engine using hadoop-2 / tez, e.g. Hive can utilize RubiX. Amazon S3 is supported.

###  How to use it

RubiX has two components: a BookKeeper server and a FileSystem implementation that an engine should use.

List of things to be done to use RubiX are:

1. Engine side changes: 
	To use RubiX, you need to place the appropriate jars in the classpath and configure Engines to use RubiX filesystem to access the cloud store. Sections below show how to get started on RubiX with supported plugins.

2. Start the BookKeeper server and LocalDataTransfer server. It can be started via `hadoop jar` command, e.g.: 
> export HADOOP_CUSTOM_OVERRIDES='fs.s3n.impl=Y29tLnF1Ym9sZS5ydWJpeC5oYWRvb3AyLkNhY2hpbmdOYXRpdmVTM0ZpbGVTeXN0ZW0=!fs.s3.impl=Y29tLnF1Ym9sZS5ydWJpeC5oYWRvb3AyLkNhY2hpbmdOYXRpdmVTM0ZpbGVTeXN0ZW0=!'

	>	hadoop jar rubix-bookkeeper-*.jar com.qubole.rubix.bookkeeper.BookKeeperServer
	
	>	hadoop jar rubix-bookkeeper-*.jar com.qubole.rubix.bookkeeper.LocalDataTransferServer
	
					OR

	>	sudo /usr/lib/hive2/bin/cache-bookkeeper start
	

NOTE: The master branch is not compatible with Hadoop1. Use RubiX version 0.2.2 or below for Hadoop1.

##### Using RubiX with Presto
  
1. Place rubix-bookkeeper.jar, rubix-core.jar, rubix-presto.jar in presto/plugin/hive-hadoop2/ directory. 
   All these jars are packaged in rubix-presto.tar under assembly module
2. Configuration changes
     i. Set configuration to use RubiX filesystem in Presto. 
     ii. Set "hive.force-local-scheduling=true" in hive.properties 
3. Start/Re-start the Presto server
		
##### Using RubiX with Hive - Hadoop1

1. Add RubiX jars:  rubix-bookkeeper.jar, rubix-core.jar, rubix-hadoop1.jar either to hadoop/lib directly or via `add jar` command. 
	    All these jars are packaged in rubix-hadoop1.tar under assembly module.
2. Configuration changes: Use following configs to start using RubiX:
		fs.s3n.impl=com.qubole.rubix.hadoop1.CachingNativeS3FileSystem   
		fs.s3.impl=com.qubole.rubix.hadoop1.CachingNativeS3FileSystem   

##### Using RubiX with Hive - Hadoop2/Tez

1. Add RubiX jars:  rubix-bookkeeper.jar, rubix-core.jar, rubix-hadoop2.jar either to /usr/lib/hadoop2/share/hadoop/common/lib/ directly or via `add jar` command. 
	    All these jars are packaged in rubix-hadoop2.tar under assembly module.
	    
2. Configuration changes: Use following configs to start using RubiX:
		fs.s3n.impl=com.qubole.rubix.hadoop2.CachingNativeS3FileSystem
		fs.s3.impl=com.qubole.rubix.hadoop2.CachingNativeS3FileSystem
		
3. Optional Configs for better Hadoop scheduling:

	a. yarn.scheduler.fair.locality.threshold.node=1.0 		
		OR    
	b. yarn.scheduler.fair.continuous-scheduling-enabled=true  
	   yarn.scheduler.fair.locality-delay-node-ms=60000  
	   yarn.scheduler.fair.locality-delay-rack-ms=60000
	   (Give time in millisecond)
	   
4. Start/Restart Hadoop2 cluster. 

### Configurations

##### BookKeeper server configurations
These configurations are to be provided as hadoop configs while starting the BookKeeper server

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
##### LocalDataTransfer server configurations
These configurations are to be provided as hadoop configs while starting the LocalDataTransfer server

| Configuration                            | Default          | Description                                                                                                                                   |
|------------------------------------------|------------------|-----------------------------------------------------------------------------------------------------------------------------------------------|
| hadoop.cache.data.local.server.port      | 8898             | The port on which LocalDataTransfer server will listen                                                                                        |                                        													       |
| hadoop.cache.data.buffer.size            | 10485760         | The size in bytes of the maximum amount of data to be read from client node in one call 													  |
| hadoop.cache.data.transfer.header.size   | 1024             | The size in bytes of the maximum size of the data header containing filepath as variable length data along with 36 bytes of fixed length data |


### Monitoring

#####Client side monitoring : Stats are published to MBean named `rubix:name=stats`

Engines which provide interface to view jmx stats can see these stats. E.g. in Presto you can run this query to see the stats:
>
```
presto:default> select * from jmx.jmx."rubix:name=stats";
    node     | cachedreads | extrareadfromremote |      hitrate       |      missrate      |   readfromcache    |  readfromremote   | remotereads | warmuppenalty 
-------------+-------------+---------------------+--------------------+--------------------+--------------------+-------------------+-------------+---------------
 presto-vbox |          25 |  1.7881784439086914 | 0.5681818181818182 | 0.4318181818181818 | 18.379225730895996 | 18.55983543395996 |          19 |             0 
(1 row)
```

#####Server side monitoring : Stats are published using a Python client. 

All engines using RubiX can see these stats. Do the following to see the stats : 

(bookkeeper.thrift is present in rubix/rubix-/spi/src/main/thrift/)

> 
```
thrift -r --gen py bookkeeper.thrift 
python getStats.py
```


### Building

mvn clean install

> Note that you will need thrift-0.9.0 installed

### Need Help?
You can post your queries to https://groups.google.com/forum/#!forum/rubix-users
