*** Settings ***
Documentation   RubiX Multi-Node Integration Tests
Resource        ..${/}shared${/}setup.robot
Resource        ..${/}shared${/}bookkeeper.robot
Library         com.qubole.rubix.client.robotframework.container.client.ContainerRequestClient

*** Variables ***
# Cache settings
${WORKINGDIR}  ${/}tmp${/}rubix${/}tests${/}NonLocalRead
${DATADIR}     ${WORKINGDIR}${/}data${/}

${HOSTNAME_WORKER1}  172.18.8.1
${HOSTNAME_WORKER2}  172.18.8.2

# Request specs
${NUM_TEST_FILES}  2

${FILEPREFIX}  ${DATADIR}${/}testFile
${TEST_FILE_1}  ${DATADIR}${/}testFile0
${TEST_FILE_2}  ${DATADIR}${/}testFile1

${FILE_LENGTH}    1048576
${LAST_MODIFIED}  1514764800
${START_BLOCK}    0
${END_BLOCK}      1048576
${CLUSTER_TYPE}   3   # TEST_CLUSTER_MANAGER

${METRIC_NONLOCAL_REQUESTS}  rubix.bookkeeper.count.nonlocal_request
${METRIC_REMOTE_REQUESTS}    rubix.bookkeeper.count.remote_request

${NUM_EXPECTED_REQUESTS_NONLOCAL}  1
${NUM_EXPECTED_REQUESTS_REMOTE}    1

*** Test Cases ***

Simple non-local read test case
    [Tags]  nonlocal
    [Documentation]  A simple non-local read test

    [Setup]  Multi-node test setup  ${DATADIR}  3

    Generate test files  ${FILEPREFIX}  ${FILE_LENGTH}  ${NUM_TEST_FILES}

    Cache data for cluster node
    ...  ${HOSTNAME_WORKER1}
    ...  ${TEST_FILE_1}
    ...  ${START_BLOCK}
    ...  ${END_BLOCK}
    ...  ${FILE_LENGTH}
    ...  ${LAST_MODIFIED}
    ...  ${CLUSTER_TYPE}

    Verify metric value on node  ${HOSTNAME_WORKER1}  ${METRIC_NONLOCAL_REQUESTS}  ${NUM_EXPECTED_REQUESTS_NONLOCAL}

    [Teardown]  Multi-node test teardown  ${DATADIR}

Simple local read test case
    [Tags]  local
    [Documentation]  A simple local read test

    [Setup]  Multi-node test setup  ${DATADIR}  3

    Generate test files  ${FILEPREFIX}  ${FILE_LENGTH}  ${NUM_TEST_FILES}

    Cache data for cluster node
    ...  ${HOSTNAME_WORKER1}
    ...  ${TEST_FILE_2}
    ...  ${START_BLOCK}
    ...  ${END_BLOCK}
    ...  ${FILE_LENGTH}
    ...  ${LAST_MODIFIED}
    ...  ${CLUSTER_TYPE}

    Verify metric value on node  ${HOSTNAME_WORKER1}  ${METRIC_REMOTE_REQUESTS}  ${NUM_EXPECTED_REQUESTS_REMOTE}

    [Teardown]  Multi-node test teardown  ${DATADIR}
