######
Presto
######

==============================
Enable RubiX Caching for Table
==============================

In order for Presto to use RubiX, you will first need to create an external table through Hive
using RubiX as the URL scheme in place of S3.

Start Hive with the following command. This will restart the metastore server,
allowing the `rubix://` scheme to be recognized::

    hive --hiveconf hive.metastore.uris="" \
         --hiveconf fs.rubix.impl=com.qubole.rubix.presto.CachingPrestoS3FileSystem

You will also need to set your AWS access & secret keys for authenticating with S3::

    hive> set fs.s3n.awsAccessKeyId=<access-key>
    hive> set fs.s3n.awsSecretAccessKey=<secret-key>

Once this is done, create your external table, but specify ``rubix://`` instead of ``s3://`` as the URL scheme:

.. code-block:: sql

    CREATE EXTERNAL TABLE...
    ...
    LOCATION 'rubix://<s3-path>'

Once your table is created, it will now be configured to use RubiX.
