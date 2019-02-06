###############
Getting Started
###############

.. NOTE::
    Make sure passwordless SSH is set up for your cluster before starting.

*************
Install RubiX
*************

Install RubiX Admin using PIP::

    pip install rubix_admin

Run ``rubix_admin -h`` to generate a config file for RubiX Admin at ``~/.radminrc``.
Add the worker node IPs to the **workers** list so the file looks like the following:

.. code-block:: yaml
   :emphasize-lines: 4-6

    coordinator:
      - localhost
    workers:
      - <worker-ip1>
      - <worker-ip2>
      ..
    remote_packages_path: /tmp/rubix_rpms

Once RubiX Admin is configured, install the latest version of RubiX on all nodes specified in ``~/.radminrc``::

    rubix_admin installer install --cluster-type <type>

To install a specific version of RubiX::

    rubix_admin installer install --cluster-type <type> --rpm-version <rubix-version>

To install from an RPM file::

    rubix_admin installer install --cluster-type <type> --rpm <path-to-rubix-rpm>

*******************
Start RubiX Daemons
*******************

Use the following command to start the BookKeeperServer and LocalDataTransferServer on all nodes specified in ``~/.radminrc``::

    rubix_admin daemon start

To verify that the daemons are running, run the following command on each node::

    sudo jps -m

You should see the following two entries in the resulting list::

    <pid> RunJar ... com.qubole.rubix.bookkeeper.BookKeeperServer
    <pid> RunJar ... com.qubole.rubix.bookkeeper.LocalDataTransferServer

If there was an issue starting the daemons, logs for RubiX can be found at ``/var/log/rubix/``.

*****************************
Configure engine to use RubiX
*****************************

Presto
======

In order for Presto to use RubiX, you will first need to create an external table through Hive
using RubiX as the URL scheme in place of S3.

Start Hive with the following command. This will restart the metastore server,
allowing the `rubix://` scheme to be recognized::

    hive --hiveconf hive.metastore.uris="" \
         --hiveconf fs.rubix.impl=com.qubole.rubix.hadoop2.CachingNativeS3FileSystem

You will also need to set your AWS access & secret keys for authenticating with S3::

    hive> set fs.s3n.awsAccessKeyId=<access-key>
    hive> set fs.s3n.awsSecretAccessKey=<secret-key>

Once this is done, create your external table, but specify ``rubix://`` instead of ``s3://`` as the URL scheme:

.. code-block:: sql

    CREATE EXTERNAL TABLE...
    ...
    LOCATION 'rubix://<s3-path>'

Once your table is created, it will now be configured to use RubiX.

Spark
=====

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

***********************************
Run your first RubiX-enhanced query
***********************************

Once you have properly configured your data engine, RubiX will now cache data when it is being fetched from S3.

You can verify this in the logs for your data engine, which should show usage of a Caching...S3FileSystem,
as well as in the BookKeeper logs at ``/var/log/rubix/bks.log``.
