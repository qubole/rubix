# RubiX

[![Build Status](https://travis-ci.org/qubole/rubix.svg?branch=master)](https://travis-ci.org/qubole/rubix)
[![Coverage Status](https://coveralls.io/repos/github/qubole/rubix/badge.svg)](https://coveralls.io/github/qubole/rubix)

RubiX is a light-weight data caching framework that can be used by Big-Data engines. Rubix uses local disks to provide
the best I/O bandwidth to the Big Data Engines. Rubix is useful in shared storage architectures where the data 
execution engine is separate from storage. For example, on public clouds like AWS or Microsoft Azure, data is stored 
in cloud store and the engine accesses the data over a network. Similarly in data centers [Presto](https://prestodb.io) 
runs on a separate cluster from HDFS and accesses data over the network.

RubiX can be extended to support any engine that accesses data using Hadoop FileSystem interface via plugins. 
There are plugins to access data on AWS S3, Microsoft Azure Blob Store and HDFS. RubiX can be extended to be 
used with any other storage systems including other cloud stores

Check the [User and Developer manual](http://rubix.readthedocs.io/en/latest/index.html) for more more information on getting started. 

## Supported Engines and Cloud Stores

- Presto: Amazon S3  
- Spark: Amazon S3  
- Any engine using hadoop-2 or hadoop-1, e.g. Hive can utilize RubiX. Amazon S3 is supported  

## Resources
[Documentation](http://rubix.readthedocs.io/en/latest/index.html)  
[User Group (Google)](https://groups.google.com/forum/#!forum/rubix-users)

### Blog Posts
[Rubix Introduction](https://www.qubole.com/blog/rubix-fast-cache-access-for-big-data-analytics-on-cloud-storage/)  
[Rubix on EMR](https://www.qubole.com/blog/caching-emr-using-rubix-performance-benchmark-benefits/)


### Developers
[Slack Channel](https://join.slack.com/t/rubix-cache/signup?x=x-348094509318-348094608182)

The channel is restricted to a few domains. Send an email on the user group or contact us through Github issues.
We will add you to the slack channel.
