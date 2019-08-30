# Developer Environment

Rubix is a Maven project and uses Java 7. It uses JUnit as the testing framework.
Ensure that you have a development environment that support the above 
configuration.

* Fork your own copy of RubiX into your GitHub account by clicking on the "Fork" button

* Navigate to your RubiX fork and clone it to your development environment
```
git clone https://github.com/<username>/rubix
```

* Add Qubole RubiX as `upstream`
```
git remote add upstream https://github.com/qubole/rubix.git
git fetch upstream
```

* Run tests in the RubiX root directory.
```
mvn test
```

* To run integration tests as well:
```
./mvnw integration-test -Pintegration-tests
```
