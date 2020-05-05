# Developer Environment

Rubix is a Maven project and uses Java 8. It uses JUnit as the testing framework.
Ensure that you have a development environment that support the above 
configuration.

## Pre-requisites

* `thrift` binary needs to be available at `/usr/local/bin/thrift`.
  For Debian-based systems:
  ```sh
  sudo apt-get install thrift-compiler
  sudo ln -s /usr/bin/thrift /usr/local/bin/thrift
  ```
  For RPM-based systems:
  ```sh
  sudo yum install epel-release
  sudo yum install thrift
  sudo ln -s /usr/bin/thrift /usr/local/bin/thrift
  ```
* Java JDK 8 needs to be used since JDK versions > 9 don't ship with `tools.jar`. So if you see an error like `Could not find artifact jdk.tools:jdk.tools:jar:1.6 at specified path` then setup your system to use JDK 8 for the build.
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
