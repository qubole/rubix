# Run your first query using RubiX

## Start Hive Client

    hive --hiveconf hive.metastore.uris="" --hiveconf fs.rubix.impl=com.qubole.rubix.hadoop2.CachingNativeS3FileSystem

## Create External Table

    CREATE EXTERNAL TABLE wikistats_orc_rubix 
    (language STRING, page_title STRING,
    hits BIGINT, retrived_size BIGINT)
    STORED AS ORC
    LOCATION 'rubix://emr.presto.airpal/wikistats/orc';

## Run Query (Presto or Hive CLI)

    SELECT language, page_title, AVG(hits) AS avg_hits
    FROM default.wikistats_orc_rubix
    WHERE language = 'en'
    AND page_title NOT IN ('Main_Page',  '404_error/')
    AND page_title NOT LIKE '%index%'
    AND page_title NOT LIKE '%Search%'
    GROUP BY language, page_title
    ORDER BY avg_hits DESC
    LIMIT 10;

## RubiX Stats (supported on Presto only)

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

