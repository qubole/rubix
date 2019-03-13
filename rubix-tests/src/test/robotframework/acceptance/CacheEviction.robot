*** Settings ***
Documentation       Rubix Cache Eviction Integration Tests
Resource            bks.robot
Suite Setup         Create Cache Parent Directories     ${CACHE_DIR_PFX}    ${CACHE_NUM_DISKS}
Suite Teardown      Remove Cache Parent Directories     ${CACHE_DIR_PFX}    ${CACHE_NUM_DISKS}

*** Variables ***
${WORKINGDIR}       ${TEMPDIR}${/}CacheEviction
${DATADIR}          ${WORKINGDIR}${/}data

${CACHE_DIR_PFX}    ${WORKINGDIR}${/}
${CACHE_DIR_SFX}    /fcache/
${CACHE_NUM_DISKS}  1
${CACHE_MAX_SIZE}   2   # MB

${METRIC_EVICTION}  rubix.bookkeeper.count.cache_eviction

${REMOTE_PATH}      ${DATADIR}${/}rubixIntegrationTestFile
${FILE_LENGTH}      1048576
${LAST_MODIFIED}    1514764800
${START_BLOCK}      0
${END_BLOCK}        1048576
${CLUSTER_TYPE}     3   # TEST_CLUSTER_MANAGER

${NUM_TEST_FILES}           5
${NUM_EXPECTED_EVICTIONS}   3

*** Test Cases ***
Test cache eviction when data downloaded
    [Documentation]     Using the BKS Thrift API, verify that files are properly evicted once the cache reaches its maximum size.
    [Tags]              eviction
    [Setup]             Cache test setup
    ...                 rubix.cluster.on-master=true
    ...                 rubix.cache.dirprefix.list=${CACHE_DIR_PFX}
    ...                 rubix.cache.dirsuffix=${CACHE_DIR_SFX}
    ...                 rubix.cache.max.disks=${CACHE_NUM_DISKS}
    ...                 rubix.cache.max.size=${CACHE_MAX_SIZE}
    [Teardown]          Cache test teardown

    @{testFiles} =      Generate test files   ${NUM_TEST_FILES}   ${FILE_LENGTH}
    :FOR    ${file}     IN      @{testFiles}
    \    Download test file data to cache    ${file}     ${START_BLOCK}   ${END_BLOCK}   ${FILE_LENGTH}   ${LAST_MODIFIED}   ${CLUSTER_TYPE}

    Verify metric value            ${METRIC_EVICTION}  ${NUM_EXPECTED_EVICTIONS}
    Verify cache directory size    ${CACHE_DIR_PFX}    ${CACHE_DIR_SFX}    ${CACHE_NUM_DISKS}    ${CACHE_MAX_SIZE}

Test cache eviction when data read
    [Documentation]     Using a caching FS, verify that files are properly evicted once the cache reaches its maximum size.
    [Tags]              eviction
    [Setup]             Cache test setup
    ...                 rubix.cluster.on-master=true
    ...                 rubix.cache.dirprefix.list=${CACHE_DIR_PFX}
    ...                 rubix.cache.dirsuffix=${CACHE_DIR_SFX}
    ...                 rubix.cache.max.disks=${CACHE_NUM_DISKS}
    ...                 rubix.cache.max.size=${CACHE_MAX_SIZE}
    [Teardown]          Cache test teardown

    @{testFiles} =      Generate test files   ${NUM_TEST_FILES}   ${FILE_LENGTH}
    :FOR    ${file}     IN      @{testFiles}
    \    Read test file data   ${file}     ${START_BLOCK}   ${END_BLOCK}

    Verify metric value            ${METRIC_EVICTION}   ${NUM_EXPECTED_EVICTIONS}
    Verify cache directory size    ${CACHE_DIR_PFX}     ${CACHE_DIR_SFX}    ${CACHE_NUM_DISKS}    ${CACHE_MAX_SIZE}

*** Keywords ***
Cache test setup
    [Arguments]                         &{options}
    Set Test Variable                   &{bksOptions}   &{options}
    Create Directory                    ${DATADIR}
    Start BKS                           &{bksOptions}
    Initialize Library Configuration    &{bksOptions}

Cache test teardown
    Stop BKS                            &{bksOptions}
    Remove Directory                    ${DATADIR}      recursive=${True}
