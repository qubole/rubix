*** Settings ***
Documentation   RubiX Multi-Node Integration Tests
Resource        setup.robot
Resource        bookkeeper.robot
Library         com.qubole.rubix.client.robotframework.driver.client.ContainerRequestClient

*** Variables ***

# Request specs
${REMOTE_PATH}      /tmp/data/testFile1
${FILE_LENGTH}      1048576
${LAST_MODIFIED}    1514764800
${START_BLOCK}      0
${END_BLOCK}        1048576
${CLUSTER_TYPE}     3   # TEST_CLUSTER_MANAGER

${TEST_FILE_1}      /tmp/data/testFile1
${TEST_FILE_2}      /tmp/data/testFile2
${TEST_FILE_3}      /tmp/data/testFile3
${TEST_FILE_4}      /tmp/data/testFile4
${TEST_FILE_5}      /tmp/data/testFile5

${PORT_MASTER_BKS}      8899
${PORT_WORKER1_BKS}     8901
${PORT_WORKER2_BKS}     8902

${PORT_MASTER_RMI}      8899
${PORT_WORKER1_RMI}     8901
${PORT_WORKER2_RMI}     8902

${METRIC_CACHE_EVICTION}    rubix.bookkeeper.count.cache_eviction
${3_EXPECTED_EVICTIONS}     3
${METRIC_NONLOCAL_REQUESTS}    rubix.bookkeeper.count.nonlocal_request

*** Test Cases ***
Multi Cache Eviction
    [Tags]  multi  eviction  multi-eviction

    Start BKS Multi

    @{testFileNames} =  CREATE LIST  ${TEST_FILE_1}  ${TEST_FILE_2}  ${TEST_FILE_3}  ${TEST_FILE_4}  ${TEST_FILE_5}
    @{requests} =  Make similar read requests
    ...  ${testFileNames}
    ...  ${START_BLOCK}
    ...  ${END_BLOCK}
    ...  ${FILE_LENGTH}
    ...  ${LAST_MODIFIED}
    ...  ${CLUSTER_TYPE}

    # Master

    Execute sequential requests on node
    ...  Download request on node
    ...  ${PORT_MASTER_BKS}
    ...  ${requests}

    Verify metric value on node
    ...  ${PORT_MASTER_BKS}
    ...  ${METRIC_CACHE_EVICTION}
    ...  ${3_EXPECTED_EVICTIONS}

    # Worker 1

    Execute sequential requests on node
    ...  Download request on node
    ...  ${PORT_WORKER1_BKS}
    ...  ${requests}

    Verify metric value on node
    ...  ${PORT_WORKER1_BKS}
    ...  ${METRIC_CACHE_EVICTION}
    ...  ${3_EXPECTED_EVICTIONS}

    # Worker 2

    Execute sequential requests on node
    ...  Download request on node
    ...  ${PORT_WORKER2_BKS}
    ...  ${requests}

    Verify metric value on node
    ...  ${PORT_WORKER2_BKS}
    ...  ${METRIC_CACHE_EVICTION}
    ...  ${3_EXPECTED_EVICTIONS}

    [Teardown]  Stop BKS Multi

Read data from container via RMI server using Thrift API
    [Tags]  rmi-read

    Start BKS Multi

    ${didRead} =  client Read Data  localhost  1901
    ...  file:${REMOTE_PATH}
    ...  ${START_BLOCK}
    ...  ${END_BLOCK}
    ...  ${FILE_LENGTH}
    ...  ${last_modified}
    ...  ${cluster_type}
    SHOULD BE TRUE  ${didRead}

    [Teardown]  Stop BKS Multi

Read data from container via RMI server using CachingFileSystem
    [Tags]  rmi-read  cfs

    Start BKS Multi

    ${didRead} =  client Read Data File System  localhost  1901
    ...  file:${REMOTE_PATH}
    ...  ${START_BLOCK}
    ...  ${END_BLOCK}
    ...  ${FILE_LENGTH}
    ...  ${last_modified}
    ...  ${cluster_type}
    SHOULD BE TRUE  ${didRead}

    [Teardown]  Stop BKS Multi

Simple non-local read test case
    [Tags]  nonlocal
    [Documentation]  A simple non-local read test

    # 1. Simple start-up & shutdown
    Start BKS Multi

    ${didRead} =  client Read Data File System  localhost  1901
    ...  file:${TEST_FILE_3}
    ...  ${START_BLOCK}
    ...  ${END_BLOCK}
    ...  ${FILE_LENGTH}
    ...  ${LAST_MODIFIED}
    ...  ${CLUSTER_TYPE}
    SHOULD BE TRUE  ${didRead}

    Verify metric value on node
    ...  ${PORT_WORKER1_BKS}
    ...  rubix.bookkeeper.count.nonlocal_request
    ...  1

    [Teardown]  Stop BKS Multi

Simple local read test case
    [Tags]  nonlocal
    [Documentation]  A simple non-local read test

    # 1. Simple start-up & shutdown
    Start BKS Multi

    ${didRead} =  client Read Data File System  localhost  1901
    ...  file:${TEST_FILE_1}
    ...  ${START_BLOCK}
    ...  ${END_BLOCK}
    ...  ${FILE_LENGTH}
    ...  ${LAST_MODIFIED}
    ...  ${CLUSTER_TYPE}
    SHOULD BE TRUE  ${didRead}

    Verify metric value on node
    ...  ${PORT_WORKER1_BKS}
    ...  rubix.bookkeeper.count.nonlocal_request
    ...  0

    Verify metric value on node
    ...  ${PORT_WORKER1_BKS}
    ...  rubix.bookkeeper.count.remote_request
    ...  1

    [Teardown]  Stop BKS Multi

#Simple Multi-Node Test Case
#    [Tags]  not-multi
#    [Documentation]  A simple multi-node test
#
#    # 1. Simple start-up & shutdown
#    Start BKS Multi
#
#    ${request} =  Make status request
#    ...  ${REMOTE_PATH}
#    ...  ${FILE_LENGTH}
#    ...  ${LAST_MODIFIED}
#    ...  ${START_BLOCK}
#    ...  ${END_BLOCK}
#    ...  ${CLUSTER_TYPE}
#
##    Download request on particular node  localhost  8899  ${request}
#    Verify metric value on node
#    ...  rubix.bookkeeper.count.total_request
#    ...  0
#    ...  localhost
#    ...  8899
#
#    # Stop BKS Multi
#
#    # Cache data on worker 2
#    # Get status for data on worker 1
#    # Cache data on worker 1 (should be non-local read from worker 2)
#    # Get metrics from worker 1 to verify that it was a non-local read
#
#    ${request3} =  Make status request
#    ...  ${TEST_FILE_3}
#    ...  ${FILE_LENGTH}
#    ...  ${LAST_MODIFIED}
#    ...  ${START_BLOCK}
#    ...  ${END_BLOCK}
#    ...  ${CLUSTER_TYPE}
#    ${locations3} =  Get status for blocks on node  localhost  ${PORT_WORKER1}  ${request3}
#    LOG MANY  ${locations3}
