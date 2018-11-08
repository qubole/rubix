*** Settings ***
Documentation   Rubix Integration Test PoC
Resource        bks.robot

*** Variables ***
${METRIC_EVICTION}  rubix.bookkeeper.count.cache_eviction

${REMOTE_PATH}      ${CURDIR}${/}rubixIntegrationTestFile
${FILE_LENGTH}      1048576
${LAST_MODIFIED}    1514764800
${START_BLOCK}      0
${END_BLOCK}        1048576
${CLUSTER_TYPE}     3   # TEST_CLUSTER_MANAGER

${CACHE_DIR_PFX}    /Users/jordanw/RobotTest/dtest/
${CACHE_DIR_SFX}    /ftest/
${CACHE_NUM_DISKS}  1
${CACHE_MAX_SIZE}   2   # MB

${NUM_TEST_FILES}           5
${NUM_EXPECTED_EVICTIONS}   3

*** Test Cases ***
Test cache eviction API
    [Documentation]     Using the BKS Thrift API, verify that files are properly evicted once the cache reaches its maximum size.
    [Tags]              cache
    [Setup]             Cache test setup
    ...                 rubix.cluster.on-master=true
    ...                 hadoop.cache.data.dirprefix.list=${CACHE_DIR_PFX}
    ...                 hadoop.cache.data.dirsuffix=${CACHE_DIR_SFX}
    ...                 hadoop.cache.data.max.disks=${CACHE_NUM_DISKS}
    ...                 hadoop.cache.data.fullness.size=${CACHE_MAX_SIZE}
    [Teardown]          Cache test teardown

    @{testFiles} =      Generate test files   ${NUM_TEST_FILES}   ${FILE_LENGTH}
    :FOR    ${file}     IN      @{testFiles}
    \    Read test file data API    ${file}     ${START_BLOCK}   ${END_BLOCK}   ${FILE_LENGTH}   ${LAST_MODIFIED}   ${CLUSTER_TYPE}

    Verify metric value         ${METRIC_EVICTION}  ${NUM_EXPECTED_EVICTIONS}
    Verify cache directories    ${CACHE_DIR_PFX}    ${CACHE_DIR_SFX}    ${CACHE_NUM_DISKS}    ${CACHE_MAX_SIZE}

Test cache eviction FS
    [Documentation]     Using a caching FS, verify that files are properly evicted once the cache reaches its maximum size.
    [Tags]              cache
    [Setup]             Cache test setup
    ...                 rubix.cluster.on-master=true
    ...                 hadoop.cache.data.dirprefix.list=${CACHE_DIR_PFX}
    ...                 hadoop.cache.data.dirsuffix=${CACHE_DIR_SFX}
    ...                 hadoop.cache.data.max.disks=${CACHE_NUM_DISKS}
    ...                 hadoop.cache.data.fullness.size=${CACHE_MAX_SIZE}
    [Teardown]          Cache test teardown

    @{testFiles} =      Generate test files   ${NUM_TEST_FILES}   ${FILE_LENGTH}
    :FOR    ${file}     IN      @{testFiles}
    \    Read test file data FS   ${file}     ${START_BLOCK}   ${END_BLOCK}

    Verify metric value         ${METRIC_EVICTION}   ${NUM_EXPECTED_EVICTIONS}
    Verify cache directories    ${CACHE_DIR_PFX}     ${CACHE_DIR_SFX}    ${CACHE_NUM_DISKS}    ${CACHE_MAX_SIZE}

*** Keywords ***
Cache test setup
    [Arguments]                 &{options}
    Set Test Variable           &{bksOptions}   &{options}
    Start BKS                   &{bksOptions}
    Clear Library Configuration
    Set Library Configuration   &{bksOptions}

Cache test teardown
    Stop BKS            &{bksOptions}
