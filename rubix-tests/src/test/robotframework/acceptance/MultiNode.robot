*** Settings ***
Documentation   RubiX Multi-Node Integration Tests
Resource        setup.robot
Resource        bookkeeper.robot
Library         com.qubole.rubix.client.robotframework.driver.client.RequestClient

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

${PORT_MASTER}      8899
${PORT_WORKER1}     8901
${PORT_WORKER2}     8902

${METRIC_CACHE_EVICTION}    rubix.bookkeeper.count.cache_eviction
${3_EXPECTED_EVICTIONS}     3

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
    ...  ${PORT_MASTER}
    ...  ${requests}

    Verify metric value on node
    ...  ${PORT_MASTER}
    ...  ${METRIC_CACHE_EVICTION}
    ...  ${3_EXPECTED_EVICTIONS}

    # Worker 1

    Execute sequential requests on node
    ...  Download request on node
    ...  ${PORT_WORKER1}
    ...  ${requests}

    Verify metric value on node
    ...  ${PORT_WORKER1}
    ...  ${METRIC_CACHE_EVICTION}
    ...  ${3_EXPECTED_EVICTIONS}

    # Worker 2

    Execute sequential requests on node
    ...  Download request on node
    ...  ${PORT_WORKER2}
    ...  ${requests}

    Verify metric value on node
    ...  ${PORT_WORKER2}
    ...  ${METRIC_CACHE_EVICTION}
    ...  ${3_EXPECTED_EVICTIONS}

Fetch cache metrics from container via RMI server
    [Tags]  rmi

    Start BKS Multi

    &{metrics} =  client Get Cache Metrics  localhost  8123
    LOG MANY  &{metrics}
    SHOULD NOT BE EMPTY  ${metrics}

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
