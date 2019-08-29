#!/bin/bash -e

build-rubix-jars-for-tests() {
    mvn clean install -DskipTests

    HADOOP_LIB_DIR="/usr/lib/hadoop2/share/hadoop/tools/lib/"
    RUBIX_JARS=$(ls ${PWD}/rubix-*/target/rubix-*.jar | grep -v tests)

    echo "=== Copying RubiX Jars to ${HADOOP_LIB_DIR} ==="
    sudo cp ${RUBIX_JARS} ${HADOOP_LIB_DIR}
}

upload-coverage-results() {
    echo "=== Running tests with coverage ==="
    # "cobertura-integration-test" goal needed for shading JARs
    # if run on CI, integration tests will have been run before this
    mvn cobertura:cobertura-integration-test
    echo "=== Uploading code coverage results to Codecov ==="
    bash <(curl -s https://codecov.io/bash)
}

build-rubix-jars-for-tests

MAVEN_CMD=$@

echo "=== Executing command \"${MAVEN_CMD}\" ==="
${MAVEN_CMD}
if [[ $? -ne 0 ]]; then
    exit 1
fi

if [[ -n "${TRAVIS}" ]]; then
    upload-coverage-results
fi
