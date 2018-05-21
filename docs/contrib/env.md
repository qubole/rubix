# Developer Environment

Rubix is a Maven project and uses Java 8. It uses JUnit as the testing framework.
Ensure that you have a development environment that support the above 
configuration.

* Fork your own copy of RubiX into your github account by clicking on the "Fork" button
* Navigate to your account and clone that copy to your development box

    
    git clone https://github.com/\<username\>/rubix


* Run tests in the RubiX root directory.
 

    mvn test
   

* Add Qubole RubiX as upstream


    git remote add upstream https://github.com/qubole/rubix.git
    git fetch upstream
