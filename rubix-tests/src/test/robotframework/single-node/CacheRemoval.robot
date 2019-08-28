*** Settings ***
Documentation   RubiX Cache Removal Integration Tests
Resource        ..${/}shared${/}setup.robot
Resource        ..${/}shared${/}bookkeeper.robot
Suite Setup     Create Cache Parent Directories  ${CACHE_DIR_PFX}  ${CACHE_NUM_DISKS}
Suite Teardown  Remove Cache Parent Directories  ${CACHE_DIR_PFX}  ${CACHE_NUM_DISKS}

*** Variables ***
# Cache settings
${WORKINGDIR}       ${TEMPDIR}${/}CacheEviction
${DATADIR}          ${WORKINGDIR}${/}data

${CACHE_DIR_PFX}    ${WORKINGDIR}${/}
${CACHE_DIR_SFX}    /fcache/
${CACHE_NUM_DISKS}  1
${CACHE_MAX_SIZE}   2   # MB

${CACHE_DIR}        ${CACHE_DIR_PFX}0${CACHE_DIR_SFX}

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

${WATCHER_DELAY}    5000

*** Test Cases ***
Cache eviction
    [Template]  Test cache eviction
    [Tags]  eviction
    Execute read request using BookKeeper server call                runConcurrently=${false}
    Concurrently execute read requests using BookKeeper server call  runConcurrently=${true}
    Execute read request using client file system                    runConcurrently=${false}
    Concurrently execute read requests using client file system      runConcurrently=${true}

Cache invalidation - last modified does not match
    [Tags]  invalidation
    [Template]  Test cache invalidation where last modified does not match
    Execute read request using BookKeeper server call                runConcurrently=${false}
    Concurrently execute read requests using BookKeeper server call  runConcurrently=${true}
    Execute read request using client file system                    runConcurrently=${false}
    Concurrently execute read requests using client file system      runConcurrently=${true}

Cache invalidation - MD exists without file
    [Template]  Test cache invalidation during async download where MD exists but file does not
    [Tags]  invalidation
    Execute read request using BookKeeper server call                runConcurrently=${false}
    Concurrently execute read requests using BookKeeper server call  runConcurrently=${true}
    Execute read request using client file system                    runConcurrently=${false}
    Concurrently execute read requests using client file system      runConcurrently=${true}

Cache expiry
    [Template]  Test cache expiry
    [Tags]  expiry
    Execute read request using BookKeeper server call                runConcurrently=${false}
    Concurrently execute read requests using BookKeeper server call  runConcurrently=${true}
    Execute read request using client file system                    runConcurrently=${false}
    Concurrently execute read requests using client file system      runConcurrently=${true}

*** Keywords ***
Test cache eviction
    [Documentation]  Verify that cache files are removed once the cache reaches its maximum size.
    [Arguments]      ${executionKeyword}  ${runConcurrently}

    # Setup
    Cache test setup
    ...  ${DATADIR}
    ...  rubix.cluster.is-master=true
    ...  rubix.cache.dirprefix.list=${CACHE_DIR_PFX}
    ...  rubix.cache.dirsuffix=${CACHE_DIR_SFX}
    ...  rubix.cache.max.disks=${CACHE_NUM_DISKS}
    ...  rubix.cache.max.size=${CACHE_MAX_SIZE}
    ...  rubix.network.server.connect.timeout=3000

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
    [Arguments]      ${executionKeyword}  ${runConcurrently}

    # Setup
    Cache test setup
    ...  ${DATADIR}
    ...  rubix.cluster.is-master=true
    ...  rubix.cache.dirprefix.list=${CACHE_DIR_PFX}
    ...  rubix.cache.dirsuffix=${CACHE_DIR_SFX}
    ...  rubix.cache.max.disks=${CACHE_NUM_DISKS}

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
    [Arguments]      ${executionKeyword}  ${runConcurrently}

    # Setup
    Cache test setup
    ...  ${DATADIR}
    ...  rubix.cluster.is-master=true
    ...  rubix.cache.dirprefix.list=${CACHE_DIR_PFX}
    ...  rubix.cache.dirsuffix=${CACHE_DIR_SFX}
    ...  rubix.cache.max.disks=${CACHE_NUM_DISKS}
    ...  rubix.cache.parallel.warmup=true
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

    ${maxWaitTime} =  EVALUATE  ${ASYNC_PROCESS_INTERVAL} + ${WATCHER_DELAY}
    ${didCache} =  Wait for cache file creation  ${CACHE_DIR}  ${maxWaitTime}  ${requests}
    SHOULD BE TRUE  ${didCache}

    Verify metric value  ${METRIC_INVALIDATION}  1
    Verify cache directory size
    ...  ${CACHE_DIR_PFX}
    ...  ${CACHE_DIR_SFX}
    ...  ${CACHE_NUM_DISKS}
    ...  expectedCacheSize=1

    [Teardown]  Cache test teardown  ${DATADIR}

Test cache expiry
    [Documentation]  Verify that cache files are removed once the cache expiry period has been reached.
    [Arguments]      ${executionKeyword}  ${runConcurrently}

    # Setup
    Cache test setup
    ...  ${DATADIR}
    ...  rubix.cluster.is-master=true
    ...  rubix.cache.dirprefix.list=${CACHE_DIR_PFX}
    ...  rubix.cache.dirsuffix=${CACHE_DIR_SFX}
    ...  rubix.cache.max.disks=${CACHE_NUM_DISKS}
    ...  rubix.cache.expiration.after-write=${CACHE_EXPIRY}
    ...  rubix.metadata.internal-cache.cleanup.enabled=true
    ...  rubix.metadata.internal-cache.cleanup.interval=1000
    ...  rubix.network.server.connect.timeout=3000

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

    ${maxWaitTime} =  EVALUATE  ${CACHE_EXPIRY} + ${WATCHER_DELAY}
    ${didCache} =  Wait for cache file removal  ${CACHE_DIR}  ${maxWaitTime}  ${requests}
    SHOULD BE TRUE  ${didCache}

    Verify metric value  ${METRIC_EXPIRY}  ${NUM_TEST_FILES}
    Verify cache directory size
    ...  ${CACHE_DIR_PFX}
    ...  ${CACHE_DIR_SFX}
    ...  ${CACHE_NUM_DISKS}
    ...  expectedCacheSize=0

    [Teardown]  Cache test teardown  ${DATADIR}