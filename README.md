# RubiX

[![Build Status](https://travis-ci.org/qubole/rubix.svg?branch=master)](https://travis-ci.org/qubole/rubix)
[![codecov](https://codecov.io/gh/qubole/rubix/branch/master/graph/badge.svg)](https://codecov.io/gh/qubole/rubix)


RubiX is a lightweight data caching framework for use with Hadoop-based data engines. 

Check the [User & Developer Manual](https://rubix.readthedocs.io/en/latest/index.html) for more information on use cases and getting started with RubiX. 

## Requirements

* Java 7
* Docker (Engine: 18.06.0+) 

## Building RubiX

To build the project, use the provided Maven wrapper:

    ./mvnw clean install

To run integration tests:

    ./mvnw clean install -DrunITs

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

