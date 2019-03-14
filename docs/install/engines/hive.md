# Hive

## Add RubiX JARs to Hadoop Classpath

Add RubiX JARs through the Hive CLI:

    hive> add jar /usr/lib/rubix/lib/rubix-hadoop2.jar
    hive> add jar /usr/lib/rubix/lib/rubix-common.jar
    hive> add jar /usr/lib/rubix/lib/rubix-core.jar
    hive> add jar /usr/lib/rubix/lib/rubix-spi.jar
    
OR

Copy JARs to Hadoop's shared lib directory:

    cp /usr/lib/rubix/lib/rubix-hadoop2.jar \ 
       /usr/lib/rubix/lib/rubix-common.jar \ 
       /usr/lib/rubix/lib/rubix-core.jar \ 
       /usr/lib/rubix/lib/rubix-spi.jar \ 
       /usr/lib/hadoop/lib/share/hadoop/tools/lib

## Configure Hive to use RubiX CachingFileSystem for S3/S3N/S3A schemes
If you use this option, all file system interactions with 
tables with their location in AWS S3 will automatically use RubiX's CachingFileSystem.

    hive --hiveconf \
        fs.s3.impl=com.qubole.rubix.hadoop2.CachingNativeS3FileSystem \
	    fs.s3n.impl=com.qubole.rubix.hadoop2.CachingNativeS3FileSystem \
        fs.s3a.impl=com.qubole.rubix.hadoop2.CachingS3AFileSystem
