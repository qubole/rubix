# RubiX

[![Build Status](https://travis-ci.org/qubole/rubix.svg?branch=master)](https://travis-ci.org/qubole/rubix)
[![codecov](https://codecov.io/gh/qubole/rubix/branch/master/graph/badge.svg)](https://codecov.io/gh/qubole/rubix)


RubiX is a lightweight data caching framework for use with Hadoop-based data engines. RubiX uses local disks to provide
the best I/O bandwidth to the Big Data Engines. RubiX is useful in shared storage architectures where the data
execution engine is separate from storage. For example, on public clouds like AWS or Microsoft Azure, data is stored
in cloud store and the engine accesses the data over a network. Similarly in data centers [Presto](https://prestosql.io)
runs on a separate cluster from HDFS and accesses data over the network.

RubiX can be extended to support any engine that accesses data in cloud stores using Hadoop FileSystem interface via plugins.
There are plugins to access data on AWS S3, Microsoft Azure Blob Store, Google Cloud Storage and HDFS. RubiX can be extended to be
used with any other storage systems including other cloud stores.

Check the [User and Developer Manual](https://rubix.readthedocs.io/en/latest/index.html) for more information on use cases and getting started with RubiX. 

## Requirements

* Java 7

For running integration tests:
* Docker (Engine: 18.06.0+) 
  * [Getting Started with Docker](https://docs.docker.com/get-started/)
  * If the above guide does not work, check out 
  [A complete one-by-one guide to install Docker on your Mac OS using Homebrew](https://medium.com/@yutafujii_59175/a-complete-one-by-one-guide-to-install-docker-on-your-mac-os-using-homebrew-e818eb4cfc3)

## Building RubiX

To build the project:

    mvn clean install

To run integration tests, use the following profiles:
* `-Psingle-node-integration-tests`: single-node tests 
* `-Pmulti-node-integration-tests`: multi-node tests
    * requires Docker & Maven wrapper `./mvnw`
* `-Pintegration-tests`: all integration tests
    * requires Docker & Maven wrapper `./mvnw`
    
Example:

    mvn clean install -Psingle-node-integration-tests
    ./mvnw clean install -Pintegration-tests

## Supported Engines and Cloud Stores

- Presto: Amazon S3  
- Spark: Amazon S3  
- Any engine using hadoop-2 e.g. Hive can utilize RubiX. Amazon S3 is supported

## Resources

### For Users
- [Documentation](https://rubix.readthedocs.io/en/latest/index.html)
- [Getting Started Guide](https://rubix.readthedocs.io/en/latest/install/getting_started.html)  
- [User Group (Google)](https://groups.google.com/forum/#!forum/rubix-users)

### For Developers
- [Contribution Guidelines](https://rubix.readthedocs.io/en/latest/contrib/index.html)  
- [Slack Channel](https://join.slack.com/t/rubix-cache/signup?x=x-348094509318-348094608182)  
The channel is restricted to a few domains. Send an email on the user group or contact us through Github issues.
We will add you to the slack channel.

### Talks & Blog Posts
- [Talk on Rubix at Strata 2017 (slides)](https://www.slideshare.net/shubhamtagra/rubix-78333181)
- [RubiX Introduction](https://www.qubole.com/blog/rubix-fast-cache-access-for-big-data-analytics-on-cloud-storage/)
- [RubiX on EMR](https://www.qubole.com/blog/caching-emr-using-rubix-performance-benchmark-benefits/)
- [Spark with RubiX](https://www.qubole.com/blog/increase-apache-spark-performance-with-rubix-distributed-cache/)
- [Presto Scheduler for RubiX Improves Cache Reads](https://www.qubole.com/blog/presto-rubix-scheduler-improves-cache-reads/)
