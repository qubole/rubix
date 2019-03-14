#####
Spark
#####

=============
Configuration
=============

In order to use Spark with S3, you will need to specify your AWS access & secret keys when running your application::

    ...
    --conf spark.hadoop.fs.s3.awsAccessKeyId=<access-key>
    --conf spark.hadoop.fs.s3.awsSecretAccessKey=<secret-key>
    ...

Alternatively, you can add the following lines to your Spark properties file to set them for every application
(default location: ``$SPARK_HOME/conf/spark-defaults.conf``)::

    spark.hadoop.fs.s3.awsAccessKeyId       <access-key>
    spark.hadoop.fs.s3.awsSecretAccessKey   <secret-key>

.. Note::
   | RubiX client configurations will also need to be set this way.
   | (Format: ``spark.hadoop.<rubix-conf-key>``)
