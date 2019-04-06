*** Settings ***
Documentation       Rubix Cache Eviction Integration Tests
Resource            setup.robot
Resource            bookkeeper.robot
Suite Setup         Create Cache Parent Directories     ${CACHE_DIR_PFX}    ${CACHE_NUM_DISKS}
Suite Teardown      Remove Cache Parent Directories     ${CACHE_DIR_PFX}    ${CACHE_NUM_DISKS}

*** Variables ***
# Cache settings
${WORKINGDIR}       ${TEMPDIR}${/}CacheEviction
${DATADIR}          ${WORKINGDIR}${/}data

${CACHE_DIR_PFX}    ${WORKINGDIR}${/}
${CACHE_DIR_SFX}    /fcache/
${CACHE_NUM_DISKS}  1
${CACHE_MAX_SIZE}   2   # MB

# Metrics
${METRIC_EVICTION}  rubix.bookkeeper.count.cache_eviction

# Request specs
${REMOTE_PATH}      ${DATADIR}${/}rubixIntegrationTestFile
${FILE_LENGTH}      1048576
${LAST_MODIFIED}    1514764800
${START_BLOCK}      0
${END_BLOCK}        1048576
${CLUSTER_TYPE}     3   # TEST_CLUSTER_MANAGER

# Test constants
${NUM_TEST_FILES}           5
${NUM_CONCURRENT_THREADS}   3

${NUM_EXPECTED_EVICTIONS}   3

*** Test Cases ***
Cache eviction
    [Template]   Test cache eviction
    Download requests               runConcurrently=${false}
    Concurrently download requests  runConcurrently=${true}
    Read requests                   runConcurrently=${false}
    Concurrently read requests      runConcurrently=${true}

*** Keywords ***
Test cache eviction
    [Documentation]  Verify that files are properly evicted once the cache reaches its maximum size.
    [Tags]           eviction
    [Arguments]      ${executionKeyword}  ${runConcurrently}

    # Setup
    Cache test setup
    ...  ${DATADIR}
    ...  rubix.cluster.is-master=true
    ...  rubix.cache.dirprefix.list=${CACHE_DIR_PFX}
    ...  rubix.cache.dirsuffix=${CACHE_DIR_SFX}
    ...  rubix.cache.max.disks=${CACHE_NUM_DISKS}
    ...  rubix.cache.max.size=${CACHE_MAX_SIZE}

    @{testFileNames} =  Generate test files  ${REMOTE_PATH}  ${FILE_LENGTH}  ${NUM_TEST_FILES}
    @{requests} =  Make similar read requests
    ...  ${testFileNames}
    ...  ${START_BLOCK}
    ...  ${END_BLOCK}
    ...  ${FILE_LENGTH}
    ...  ${LAST_MODIFIED}

    RUN KEYWORD IF  ${runConcurrently}
    ...  Execute concurrent requests
    ...  ${executionKeyword}
    ...  ${NUM_CONCURRENT_THREADS}
    ...  ${requests}
    ...  ELSE
    ...  Execute sequential requests
    ...  ${executionKeyword}
    ...  ${requests}

    Verify metric value  ${METRIC_EVICTION}  ${NUM_EXPECTED_EVICTIONS}
    Verify cache directory size
    ...  ${CACHE_DIR_PFX}
    ...  ${CACHE_DIR_SFX}
    ...  ${CACHE_NUM_DISKS}
    ...  expectedCacheSize=${CACHE_MAX_SIZE}

    [Teardown]  Cache test teardown  ${DATADIR}