*** Settings ***
Documentation       RubiX Cache Removal Integration Tests
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
${METRIC_EVICTION}      rubix.bookkeeper.count.cache_eviction
${METRIC_EXPIRY}        rubix.bookkeeper.count.cache_expiry
${METRIC_INVALIDATION}  rubix.bookkeeper.count.cache_invalidation

# Request specs
${REMOTE_PATH}      ${DATADIR}${/}rubixIntegrationTestFile
${FILE_LENGTH}      1048576
${LAST_MODIFIED}    1514764800
${START_BLOCK}      0
${END_BLOCK}        1048576
${CLUSTER_TYPE}     3   # TEST_CLUSTER_MANAGER

${LAST_MODIFIED_JAN_1_2018}   1514764800
${LAST_MODIFIED_JAN_2_2018}   1514851200

# Test constants
${NUM_TEST_FILES}           5
${NUM_CONCURRENT_THREADS}   3

${NUM_EXPECTED_EVICTIONS}   3

${CACHE_EXPIRY}             1000
${ASYNC_PROCESS_INTERVAL}   500
${ASYNC_PROCESS_DELAY}      1

*** Test Cases ***
Cache eviction
    [Template]  Test cache eviction
    Download requests               runConcurrently=${false}
    Concurrently download requests  runConcurrently=${true}
    Read requests                   runConcurrently=${false}
    Concurrently read requests      runConcurrently=${true}

Cache invalidation - last modified does not match
    [Template]  Test cache invalidation where last modified does not match
    Download requests               runConcurrently=${false}
    Concurrently download requests  runConcurrently=${true}
    Read requests                   runConcurrently=${false}
    Concurrently read requests      runConcurrently=${true}

Cache invalidation - MD exists without file
    [Template]  Test cache invalidation during async download where MD exists but file does not
    Download requests               runConcurrently=${false}
    Concurrently download requests  runConcurrently=${true}
    Read requests                   runConcurrently=${false}
    Concurrently read requests      runConcurrently=${true}

Cache expiry
    [Template]  Test cache expiry
    Download requests               runConcurrently=${false}
    Concurrently download requests  runConcurrently=${true}
    Read requests                   runConcurrently=${false}
    Concurrently read requests      runConcurrently=${true}

*** Keywords ***
Test cache eviction
    [Documentation]  Verify that cache files are removed once the cache reaches its maximum size.
    [Tags]           eviction
    [Arguments]      ${executionKeyword}  ${runConcurrently}

    # Setup
    Cache test setup
    ...  ${DATADIR}
    ...  rubix.cluster.on-master=true
    ...  hadoop.cache.data.dirprefix.list=${CACHE_DIR_PFX}
    ...  hadoop.cache.data.dirsuffix=${CACHE_DIR_SFX}
    ...  hadoop.cache.data.max.disks=${CACHE_NUM_DISKS}
    ...  rubix.cache.fullness.size=${CACHE_MAX_SIZE}

    @{testFileNames} =  Generate test files  ${REMOTE_PATH}  ${FILE_LENGTH}  ${NUM_TEST_FILES}
    @{requests} =  Make similar read requests
    ...  ${testFileNames}
    ...  ${START_BLOCK}
    ...  ${END_BLOCK}
    ...  ${FILE_LENGTH}
    ...  ${LAST_MODIFIED}
    ...  ${CLUSTER_TYPE}

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

Test cache invalidation where last modified does not match
    [Documentation]  Verify that cache files are removed when requesting status for a file with a newer modified date than what is cached.
    [Tags]           eviction
    [Arguments]      ${executionKeyword}  ${runConcurrently}

    # Setup
    Cache test setup
    ...  ${DATADIR}
    ...  rubix.cluster.on-master=true
    ...  hadoop.cache.data.dirprefix.list=${CACHE_DIR_PFX}
    ...  hadoop.cache.data.dirsuffix=${CACHE_DIR_SFX}
    ...  hadoop.cache.data.max.disks=${CACHE_NUM_DISKS}

    @{testFileNames} =  Generate test files  ${REMOTE_PATH}  ${FILE_LENGTH}  ${NUM_TEST_FILES}
    @{requests} =  Make similar read requests
    ...  ${testFileNames}
    ...  ${START_BLOCK}
    ...  ${END_BLOCK}
    ...  ${FILE_LENGTH}
    ...  ${LAST_MODIFIED_JAN_1_2018}
    ...  ${CLUSTER_TYPE}

    RUN KEYWORD IF  ${runConcurrently}
    ...  Execute concurrent requests
    ...  ${executionKeyword}
    ...  ${NUM_CONCURRENT_THREADS}
    ...  ${requests}
    ...  ELSE
    ...  Execute sequential requests
    ...  ${executionKeyword}
    ...  ${requests}

    ${statusRequest} =  Make status request
    ...  ${testFileNames[0]}
    ...  ${FILE_LENGTH}
    ...  ${LAST_MODIFIED_JAN_2_2018}
    ...  ${START_BLOCK}
    ...  ${END_BLOCK}
    ...  ${CLUSTER_TYPE}

    Get status for blocks  ${statusRequest}

    Verify metric value  ${METRIC_INVALIDATION}  1
    Verify cache directory size
    ...  ${CACHE_DIR_PFX}
    ...  ${CACHE_DIR_SFX}
    ...  ${CACHE_NUM_DISKS}
    ...  expectedCacheSize=4

    [Teardown]  Cache test teardown  ${DATADIR}

Test cache invalidation during async download where MD exists but file does not
    [Documentation]  Verify that cache files are removed if metadata exists without a matching cache file during asynchronous downloads.
    [Tags]           eviction
    [Arguments]      ${executionKeyword}  ${runConcurrently}

    # Setup
    Cache test setup
    ...  ${DATADIR}
    ...  rubix.cluster.on-master=true
    ...  hadoop.cache.data.dirprefix.list=${CACHE_DIR_PFX}
    ...  hadoop.cache.data.dirsuffix=${CACHE_DIR_SFX}
    ...  hadoop.cache.data.max.disks=${CACHE_NUM_DISKS}
    ...  rubix.parallel.warmup=true
    ...  rubix.request.process.initial.delay=${ASYNC_PROCESS_INTERVAL}
    ...  rubix.request.process.interval=${ASYNC_PROCESS_INTERVAL}
    ...  rubix.remotefetch.interval=${ASYNC_PROCESS_DELAY}

    ${testFileName} =  Generate single test file  ${REMOTE_PATH}  ${FILE_LENGTH}

    # Check cache status of file to add FileMetadata object to BookKeeper metadata cache
    ${statusRequest} =  Make status request
    ...  ${testFileName}
    ...  ${FILE_LENGTH}
    ...  ${LAST_MODIFIED_JAN_1_2018}
    ...  ${START_BLOCK}
    ...  ${END_BLOCK}
    ...  ${CLUSTER_TYPE}
    Get status for blocks  ${statusRequest}

    # Manually generate MD file to satisfy test condition (MD file exists without cache file)
    Generate MD File  ${testFileName}

    ${request} =  Make read request
    ...  ${testFileName}
    ...  ${START_BLOCK}
    ...  ${END_BLOCK}
    ...  ${FILE_LENGTH}
    ...  ${LAST_MODIFIED_JAN_1_2018}
    ...  ${CLUSTER_TYPE}
    @{requests} =  CREATE LIST  ${request}

    RUN KEYWORD IF  ${runConcurrently}
    ...  Execute concurrent requests
    ...  ${executionKeyword}
    ...  ${NUM_CONCURRENT_THREADS}
    ...  ${requests}
    ...  ELSE
    ...  Execute sequential requests
    ...  ${executionKeyword}
    ...  ${requests}

    ${waitTime} =  EVALUATE  ${ASYNC_PROCESS_INTERVAL} * 4
    SLEEP  ${waitTime}ms  Wait for async request to process.

    Verify metric value  ${METRIC_INVALIDATION}  1
    Verify cache directory size
    ...  ${CACHE_DIR_PFX}
    ...  ${CACHE_DIR_SFX}
    ...  ${CACHE_NUM_DISKS}
    ...  expectedCacheSize=1

    [Teardown]  Cache test teardown  ${DATADIR}

Test cache expiry
    [Documentation]  Verify that cache files are removed once the cache expiry period has been reached.
    [Tags]           eviction
    [Arguments]      ${executionKeyword}  ${runConcurrently}

    # Setup
    Cache test setup
    ...  ${DATADIR}
    ...  rubix.cluster.on-master=true
    ...  hadoop.cache.data.dirprefix.list=${CACHE_DIR_PFX}
    ...  hadoop.cache.data.dirsuffix=${CACHE_DIR_SFX}
    ...  hadoop.cache.data.max.disks=${CACHE_NUM_DISKS}
    ...  hadoop.cache.data.expiration.after-write=${CACHE_EXPIRY}

    @{testFileNames} =  Generate test files  ${REMOTE_PATH}  ${FILE_LENGTH}  ${NUM_TEST_FILES}
    @{requests} =  Make similar read requests
    ...  ${testFileNames}
    ...  ${START_BLOCK}
    ...  ${END_BLOCK}
    ...  ${FILE_LENGTH}
    ...  ${LAST_MODIFIED}
    ...  ${CLUSTER_TYPE}

    RUN KEYWORD IF  ${runConcurrently}
    ...  Execute concurrent requests
    ...  ${executionKeyword}
    ...  ${NUM_CONCURRENT_THREADS}
    ...  ${requests}
    ...  ELSE
    ...  Execute sequential requests
    ...  ${executionKeyword}
    ...  ${requests}

    Verify metric value  ${METRIC_EXPIRY}  0
    Verify cache directory size
    ...  ${CACHE_DIR_PFX}
    ...  ${CACHE_DIR_SFX}
    ...  ${CACHE_NUM_DISKS}
    ...  expectedCacheSize=${NUM_TEST_FILES}

    SLEEP  ${CACHE_EXPIRY}ms

    Verify metric value  ${METRIC_EXPIRY}  ${NUM_TEST_FILES}
    Verify cache directory size
    ...  ${CACHE_DIR_PFX}
    ...  ${CACHE_DIR_SFX}
    ...  ${CACHE_NUM_DISKS}
    ...  expectedCacheSize=0

    [Teardown]  Cache test teardown  ${DATADIR}