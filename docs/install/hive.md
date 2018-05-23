# Apache Hive

## Add RubiX jars to Hadoop ClassPath

Copy jars to Hadoop Lib

    cp /usr/lib/rubix/lib/rubix-* /usr/lib/hadoop/lib 
    
OR

Add Rubix Jars

    add jar /usr/lib/rubix/lib/rubix-bookkeeper.jar
    add jar /usr/lib/rubix/lib/rubix-core.jar
    add jar /usr/lib/rubix/lib/rubix-hadoop2.jar

## Restart Hive Metastore Server

    hive --service metastore --stop
    hive --service metastore --start

## Configure Apache Hive to use Rubix FileSystem

    hive --hiveconf \
	    fs.rubix.impl=com.qubole.rubix.hadoop2.CachingNativeS3FileSystem \
	    fs.rubix.awsAccessKeyId=<AWS ACCESS KEY> \
	    fs.rubix.awsSecretAccessKey=<AWS SECRET ACCESS KEY>

## (Advanced) Configure Apache Hive to use Rubix FileSystem for S3 and S3A schemes
    hive --hiveconf \
	    fs.s3n.impl=com.qubole.rubix.hadoop2.CachingNativeS3FileSystem
            fs.s3.impl=com.qubole.rubix.hadoop2.CachingNativeS3FileSystem
            fs.s3a.impl=com.qubole.rubix.hadoop2.CachingS3AFileSystem
