# Start and connect to master

## Start an EMR Cluster

Create an EMR cluster, use 'advanced option' to create a cluster with the required spec.
1. Specify at least 30GB space for Root device EBS volume.
2. To login into cluster, choose a EC2 key pair.

## Connect to Master Node

Log into master and all worker nodes as "hadoop" user.

    ssh <key> hadoop@master-public-ip and install required libs.

Setup and check passwordless SSH between cluster machines

    ssh hadoop@localhost
    ssh hadoop@worker-public-ip
