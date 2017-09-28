# RubiX
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
