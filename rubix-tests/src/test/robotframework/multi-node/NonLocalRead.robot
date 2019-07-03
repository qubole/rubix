*** Settings ***
Documentation   RubiX Multi-Node Integration Tests
Resource        ..${/}shared${/}setup.robot
Resource        ..${/}shared${/}bookkeeper.robot
Library         com.qubole.rubix.client.robotframework.container.client.ContainerRequestClient

*** Variables ***
# Cache settings
${WORKINGDIR}  ${CURDIR}${/}files${/}NonLocalRead
${DATADIR}     ${WORKINGDIR}${/}data${/}

${FILEPREFIX}  ${DATADIR}${/}testFile

# Request specs
${TEST_FILE_1}  /tmp/data/testFile1
${TEST_FILE_2}  /tmp/data/testFile2
${TEST_FILE_3}  /tmp/data/testFile3
${TEST_FILE_4}  /tmp/data/testFile4
${TEST_FILE_5}  /tmp/data/testFile5

${FILE_LENGTH}    1048576
${LAST_MODIFIED}  1514764800
${START_BLOCK}    0
${END_BLOCK}      1048576
${CLUSTER_TYPE}   3   # TEST_CLUSTER_MANAGER

${NUM_TEST_FILES}  5

${PORT_WORKER1_REQUEST_SERVER}  1901
${PORT_WORKER2_REQUEST_SERVER}  1902

${METRIC_NONLOCAL_REQUESTS}  rubix.bookkeeper.count.nonlocal_request
${METRIC_REMOTE_REQUESTS}    rubix.bookkeeper.count.remote_request

${NUM_EXPECTED_REQUESTS_NONLOCAL}  1
${NUM_EXPECTED_REQUESTS_REMOTE}    1

*** Test Cases ***
Simple non-local read test case
    [Tags]  nonlocal
    [Documentation]  A simple non-local read test

    [Setup]  Multi-node test setup  ${DATADIR}

    Generate test files  ${FILEPREFIX}  ${FILE_LENGTH}  ${NUM_TEST_FILES}  1

    Cache data for cluster node  ${PORT_WORKER1_REQUEST_SERVER}
    ...  localhost
    ...  file:${TEST_FILE_3}
    ...  ${START_BLOCK}
    ...  ${END_BLOCK}
    ...  ${FILE_LENGTH}
    ...  ${LAST_MODIFIED}
    ...  ${CLUSTER_TYPE}

    Verify metric value on node  localhost  ${PORT_WORKER1_REQUEST_SERVER}  ${METRIC_NONLOCAL_REQUESTS}  ${NUM_EXPECTED_REQUESTS_NONLOCAL}

    [Teardown]  Multi-node test teardown  ${DATADIR}

Simple local read test case
    [Tags]  local
    [Documentation]  A simple local read test

    [Setup]  Multi-node test setup  ${DATADIR}

    Generate test files  ${FILEPREFIX}  ${FILE_LENGTH}  ${NUM_TEST_FILES}  1

    Cache data for cluster node  ${PORT_WORKER1_REQUEST_SERVER}
    ...  localhost
    ...  file:${TEST_FILE_1}
    ...  ${START_BLOCK}
    ...  ${END_BLOCK}
    ...  ${FILE_LENGTH}
    ...  ${LAST_MODIFIED}
    ...  ${CLUSTER_TYPE}

    Verify metric value on node  localhost  ${PORT_WORKER1_REQUEST_SERVER}  ${METRIC_REMOTE_REQUESTS}  ${NUM_EXPECTED_REQUESTS_REMOTE}

    [Teardown]  Multi-node test teardown  ${DATADIR}
