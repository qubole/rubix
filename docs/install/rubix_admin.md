# Install and start RubiX

## Install RubiX Admin

    pip install rubix_admin

## Update Config File

    rubix_admin -h

This will create rubix-admin config file at ~/.radminrc with the follwoing format

    hosts:
      - localhost
      - worker-ip1
      - worker-ip2
      ..
    remote_packages_path: /tmp/rubix_rpms

## Install RubiX
    rubix_admin installer install

This will install the latest version of RubiX. To install a specific version of Rubix,

    rubix_admin installer install --rpm-version <RubiX Version>

To install from a rpm file,

    rubix_admin installer install --rpm <path-to-rubix-rpm> 

To enable debugging and see the rubix activity, create /usr/lib/presto/etc/log.properties file with bellow config.
com.qubole=DEBUG

## Start RubiX Daemons
    rubix_admin daemon start --debug
    # To verfiy the daemons are up
       verfiy process ids for both 
       BookKeeperServer and LocalDiscoveryServer.
    sudo jps -m

