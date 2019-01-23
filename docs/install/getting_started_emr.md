# Getting Started on EMR

## Start an EMR cluster

Select 'Go to advanced options' to create a cluster with the required specs:

### Step 1: Software
* **Release:** Choose the latest EMR 5.x version 
* Make sure 'Hadoop x.x.x' is checked.
* Make sure Presto or Spark is checked, depending on which type of cluster you want to set up.

### Step 2: Hardware
* **Root device EBS volume size:** Specify at least 30 GiB.

### Step 4: Security
* **EC2 key pair:** Choose a key pair to use for connecting to the cluster via SSH.

## Set up passwordless SSH

Copy your EC2 key pair to the master node of the EMR cluster, then log into the master node as `hadoop` using the same key pair.

    scp -i <PEM-file> <PEM-file> hadoop@<master-public-ip>:/tmp
    ssh -i <PEM-file> hadoop@<master-public-ip>

Fetch the list of IPs for the worker nodes in the cluster:

    hdfs dfsadmin -report | grep ^Name

Now that you have the IPs for the worker nodes, use the following commands to generate a

    ssh-agent bash
    ssh-add <PEM-file>
    ssh-keygen
    (specify options, or press enter until finished)

Copy the public key you generated to each of the worker nodes

    ssh-copy-id -i ~/.ssh/id_rsa.pub hadoop@<worker-IP>

The following commands should successfully connect you to the desired node, without requiring a password

    ssh hadoop@localhost
    ssh hadoop@<worker-IP>

## Install RubiX

Install RubiX Admin using PIP:

    pip install rubix_admin

Run `rubix_admin -h` to generate a config file for Rubix Admin at `~/.radminrc`.
Add the worker IPs you fetched when setting up passwordless SSH to the workers list so the file looks like the following:

    coordinator:
      - localhost
    workers:
      - <worker-ip1>
      - <worker-ip2>
      ..
    remote_packages_path: /tmp/rubix_rpms

Once RubiX Admin is configured, use the following command to install the latest version of RubiX on all nodes specified in `~/.radminrc`.

    rubix_admin installer install --cluster-type <type>

To install a specific version of RubiX:

    rubix_admin installer install --cluster-type <type> --rpm-version <rubix-version>

To install from an RPM file:

    rubix_admin installer install --cluster-type <type> --rpm <path-to-rubix-rpm>

## Start RubiX Daemons

Use the following command to start the BookKeeperServer and LocalDataTransferServer on all nodes specified in `~/.radminrc`:

    rubix_admin daemon start

To verify that the daemons are running, run the following command on each node:

    sudo jps -m

You should see the following two entries in the resulting list:

    <pid> RunJar ... com.qubole.rubix.bookkeeper.BookKeeperServer
    <pid> RunJar ... com.qubole.rubix.bookkeeper.LocalDataTransferServer

If there was an issue starting the daemons, logs for RubiX can be found at `/var/log/rubix/`.

## Configure engine to use RubiX

### Presto

In order for Presto to use RubiX, you will first need to create an external table through Hive
using RubiX as the URL scheme in place of S3.

Start Hive with the following command. This will restart the metastore server,
allowing the `rubix://` scheme to be recognized.
<!-- empty line for formatting -->

    hive --hiveconf hive.metastore.uris="" \
        --hiveconf fs.rubix.impl=com.qubole.rubix.hadoop2.CachingNativeS3FileSystem

You will also need to set your AWS access & secret keys for authenticating with S3:

    hive> set fs.s3n.awsAccessKeyId=<access-key>
    hive> set fs.s3n.awsSecretAccessKey=<secret-key>

Once this is done, create your external table, but specify `rubix://` instead of `s3://` as the URL scheme

    CREATE EXTERNAL TABLE...
    ...
    LOCATION 'rubix://<s3-path>'

Once your table is linked, it should now be configured to use RubiX!

### Spark

In order to use Spark with S3, you will need to specify your AWS access & secret keys when running your application:

    ...
    --conf spark.hadoop.fs.s3.awsAccessKeyId=<access-key>
    --conf spark.hadoop.fs.s3.awsSecretAccessKey=<secret-key>
    ...

Alternatively, you can add the following lines to your Spark properties file to supply them for every application:
(default location: `/etc/spark/conf/spark-defaults.conf`)

    spark.hadoop.fs.s3.awsAccessKeyId       <access-key>
    spark.hadoop.fs.s3.awsSecretAccessKey   <secret-key>

## Run your first Rubix-enhanced query

Once you properly configure your data engine, RubiX should now cache data when it is being fetched from S3.

You can verify this through the logs for your data engine, which should show usage of a Caching...S3FileSystem,
as well as through the BookKeeper logs at `/var/log/rubix/bks.log`
