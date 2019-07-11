#!/bin/bash -e

MAVEN_CMD=$@

build-rubix-jars-for-tests() {
#    echo $PWD
#    mvn clean install -DskipTests --projects '-assembly,-rubix-rpm'
    mvn clean install -DskipTests

    HADOOP_JARSPATH="/usr/lib/hadoop2/share/hadoop/tools/lib/"
    RUBIX_JARS=$(ls ${PWD}/rubix-*/target/rubix-*.jar | grep -v tests)

    echo "=== Copying RubiX Jars to ${HADOOP_JARSPATH} ==="
    sudo cp ${RUBIX_JARS} ${HADOOP_JARSPATH}
}

build-rubix-jars-for-tests

echo "=== Executing command \"${MAVEN_CMD}\" ==="
exec ${MAVEN_CMD}
#mvn clean install -rf :rubix-tests -Dincludes=local
if [[ $? -ne 0 ]]; then
    exit 1
fi
#mvn cobertura:cobertura-integration-test

if [[ -n "${TRAVIS}" ]]; then
    echo "=== Uploading code coverage results to Codecov ==="
    bash <(curl -s https://codecov.io/bash)
fi
