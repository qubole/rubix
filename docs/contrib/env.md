# Developer Environment

Rubix is a Maven project and uses Java 8. It uses JUnit as the testing framework.
Ensure that you have a development environment that support the above 
configuration.

## Pre-requisites

* `thrift` binary needs to be available at `/usr/local/bin/thrift`. Rubix will not compile with the newer versions of thrift, it is recommended to install thrift version 0.9.3 by downloading the source from [here](http://apache.mirrors.spacedump.net/thrift/0.9.3/thrift-0.9.3.tar.gz) and installing it using the steps mentioned [here](https://thrift.apache.org/docs/BuildingFromSource)

* Java JDK 8 needs to be used. If you see an error like `Fatal error compiling: invalid target release: 1.8` during compilation then setup your system to use Java JDK 8 for the build.

* For generating the RPM you need the `rpmbuild` command available. On Debian-based systems `sudo apt-get install rpmbuild` and on RPM-based systems `sudo yum install rpm-build` make it available.

## Building

* Fork your own copy of RubiX into your github account by clicking on the "Fork" button
* Navigate to your account and clone that copy to your development box

    
    git clone https://github.com/\<username\>/rubix


* Run tests in the RubiX root directory.
 

    mvn test
   

* Add Qubole RubiX as upstream


    git remote add upstream https://github.com/qubole/rubix.git
    git fetch upstream
