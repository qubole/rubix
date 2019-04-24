*** Settings ***
Documentation       Rubix Asynchronous Download Integration Tests
Resource            setup.robot
Resource            bookkeeper.robot
Suite Setup         Create Cache Parent Directories     ${CACHE_DIR_PFX}    ${CACHE_NUM_DISKS}
Suite Teardown      Remove Cache Parent Directories     ${CACHE_DIR_PFX}    ${CACHE_NUM_DISKS}

*** Variables ***
# Cache settings
${WORKINGDIR}       ${TEMPDIR}${/}AsyncWarmup
${DATADIR}          ${WORKINGDIR}${/}data

${CACHE_DIR_PFX}    ${WORKINGDIR}${/}
${CACHE_DIR_SFX}    /fcache/
${CACHE_NUM_DISKS}  1

# Metrics
${METRIC_ASYNC_QUEUE_SIZE}          rubix.bookkeeper.gauge.async_queue_size
${METRIC_ASYNC_PROCESSED_REQUESTS}  rubix.bookkeeper.count.processed_async_request
${METRIC_ASYNC_TOTAL_REQUESTS}      rubix.bookkeeper.count.total_async_request
${METRIC_ASYNC_DOWNLOADED_MB}       rubix.bookkeeper.count.async_downloaded_mb

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
${NUM_CONCURRENT_THREADS}   2

${ASYNC_PROCESS_INTERVAL}               1000
${ASYNC_PROCESS_DELAY}                  10
${ASYNC_PROCESS_INTERVAL_SOME_DELAYED}  3000
${ASYNC_PROCESS_DELAY_SOME_DELAYED}     1500

*** Test Cases ***
Async caching
    [Documentation]  Verify that files are correctly cached when asynchronously downloaded.
    [Template]  Test async caching
    Download requests               runConcurrently=${false}
    Concurrently download requests  runConcurrently=${true}
    Read requests                   runConcurrently=${false}
    Concurrently read requests      runConcurrently=${true}

Async caching - Some requests delayed
    [Documentation]  Verify that asynchronous caching only downloads files queued outside of the delay period.
    [Template]  Test async caching with some requests delayed
    Download requests               runConcurrently=${false}
    Concurrently download requests  runConcurrently=${true}
    Read requests                   runConcurrently=${false}
    Concurrently read requests      runConcurrently=${true}

Async caching - Request 1 file date before Request 2
    [Documentation]  Verify that later read requests for the same file with a later last-modified date are handled correctly.
    [Template]  Test async caching with request 1 file date before request 2
    Download requests               runConcurrently=${false}
    Concurrently download requests  runConcurrently=${true}

Async caching - Request 1 file date after Request 2
    [Documentation]  Verify that later read requests for the same file with an earlier last-modified date are handled correctly.
    [Template]  Test async caching with request 1 file date after request 2
    Download requests               runConcurrently=${false}
    Concurrently download requests  runConcurrently=${true}

*** Keywords ***
Test async caching
    [Documentation]  Verify that files are correctly cached when asynchronously downloaded.
    [Tags]           async
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

    @{testFiles} =  Generate test files  ${REMOTE_PATH}  ${FILE_LENGTH}  ${NUM_TEST_FILES}
    @{requests} =  Make similar read requests
    ...  ${testFiles}
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

    Verify metric value  ${METRIC_ASYNC_QUEUE_SIZE}  ${NUM_TEST_FILES}

    ${waitTime} =  EVALUATE  ${ASYNC_PROCESS_INTERVAL} * ${NUM_TEST_FILES}
    SLEEP  ${waitTime}ms  Wait for queued requests to finish

    Verify async metrics
    ...  queueSize=0
    ...  processedRequests=${NUM_TEST_FILES}
    ...  totalRequests=${NUM_TEST_FILES}
    ...  downloadedMB=${NUM_TEST_FILES}
    Verify cache directory size
    ...  ${CACHE_DIR_PFX}
    ...  ${CACHE_DIR_SFX}
    ...  ${CACHE_NUM_DISKS}
    ...  expectedCacheSize=${NUM_TEST_FILES}

    [Teardown]  Cache test teardown  ${DATADIR}

Test async caching with some requests delayed
    [Documentation]  Verify that asynchronous caching only downloads files queued outside of the delay period.
    [Tags]           async
    [Arguments]      ${executionKeyword}  ${runConcurrently}

    # Setup
    Cache test setup
    ...  ${DATADIR}
    ...  rubix.cluster.is-master=true
    ...  rubix.cache.dirprefix.list=${CACHE_DIR_PFX}
    ...  rubix.cache.dirsuffix=${CACHE_DIR_SFX}
    ...  rubix.cache.max.disks=${CACHE_NUM_DISKS}
    ...  rubix.cache.parallel.warmup=true
    ...  rubix.request.process.initial.delay=${ASYNC_PROCESS_INTERVAL_SOME_DELAYED}
    ...  rubix.request.process.interval=${ASYNC_PROCESS_INTERVAL_SOME_DELAYED}
    ...  rubix.remotefetch.interval=${ASYNC_PROCESS_DELAY_SOME_DELAYED}

    ${numFilesFirstPass} =  SET VARIABLE  3
    ${numFilesSecondPass} =  SET VARIABLE  2
    ${numTotalFiles} =  EVALUATE  ${numFilesFirstPass} + ${numFilesSecondPass}

    @{testFilesFirstPass} =  Generate test files  ${DATADIR}${/}firstPass  ${FILE_LENGTH}  ${numFilesFirstPass}
    @{requests} =  Make similar read requests
    ...  ${testFilesFirstPass}
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

    Verify async metrics
    ...  queueSize=${numFilesFirstPass}
    ...  processedRequests=0
    ...  totalRequests=${numFilesFirstPass}
    ...  downloadedMB=0

    ${delayForSecondPass} =  EVALUATE  ${ASYNC_PROCESS_INTERVAL_SOME_DELAYED} - ${ASYNC_PROCESS_DELAY_SOME_DELAYED} + 1
    SLEEP  ${delayForSecondPass}ms  Hold for second set so next files are postponed due to process delay

    @{testFilesSecondPass} =  Generate test files  ${DATADIR}${/}secondPass  ${FILE_LENGTH}  ${numFilesSecondPass}
    @{requests} =  Make similar read requests
    ...  ${testFilesSecondPass}
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

    Verify async metrics
    ...  queueSize=${numTotalFiles}
    ...  processedRequests=0
    ...  totalRequests=${numTotalFiles}
    ...  downloadedMB=0

    SLEEP  ${ASYNC_PROCESS_DELAY_SOME_DELAYED}ms  Wait for first-pass files to process

    Verify async metrics
    ...  queueSize=${numFilesSecondPass}
    ...  processedRequests=${numFilesFirstPass}
    ...  totalRequests=${numTotalFiles}
    ...  downloadedMB=${numFilesFirstPass}

    SLEEP  ${ASYNC_PROCESS_INTERVAL_SOME_DELAYED}ms  Wait another interval for remaining files to process

    Verify async metrics
    ...  queueSize=0
    ...  processedRequests=${numTotalFiles}
    ...  totalRequests=${numTotalFiles}
    ...  downloadedMB=${numTotalFiles}
    Verify cache directory size
    ...  ${CACHE_DIR_PFX}
    ...  ${CACHE_DIR_SFX}
    ...  ${CACHE_NUM_DISKS}
    ...  expectedCacheSize=${numTotalFiles}
    [Teardown]  Cache test teardown  ${DATADIR}

Test async caching with request 1 file date before request 2
    [Documentation]  Verify that later read requests for the same file with a later last-modified date are handled correctly.
    [Tags]           async
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

    ${testFile} =  Generate single test file  ${DATADIR}${/}testFile1  ${FILE_LENGTH}

    ${readRequestJan1} =  Make read request
    ...  ${testFile}
    ...  ${START_BLOCK}
    ...  ${END_BLOCK}
    ...  ${FILE_LENGTH}
    ...  ${LAST_MODIFIED_JAN_1_2018}
    ...  ${CLUSTER_TYPE}
    ${readRequestJan2} =  Make read request
    ...  ${testFile}
    ...  ${START_BLOCK}
    ...  ${END_BLOCK}
    ...  ${FILE_LENGTH}
    ...  ${LAST_MODIFIED_JAN_2_2018}
    ...  ${CLUSTER_TYPE}
    @{requests} =  CREATE LIST  ${readRequestJan1}  ${readRequestJan2}

    RUN KEYWORD IF  ${runConcurrently}
    ...  Execute concurrent requests
    ...  ${executionKeyword}
    ...  ${NUM_CONCURRENT_THREADS}
    ...  ${requests}
    ...  staggerRequests=${true}
    ...  ELSE
    ...  Execute sequential requests
    ...  ${executionKeyword}
    ...  ${requests}

    ${totalRequests} =  GET LENGTH  ${requests}
    Verify metric value  ${METRIC_ASYNC_QUEUE_SIZE}  ${totalRequests}

    ${waitTime} =  EVALUATE  ${ASYNC_PROCESS_INTERVAL} * ${totalRequests} * 6
    SLEEP  ${waitTime}ms  Wait for queued requests to finish

    Verify async metrics
    ...  queueSize=0
    ...  processedRequests=${totalRequests}
    ...  totalRequests=${totalRequests}
    ...  downloadedMB=1
    Verify cache directory size
    ...  ${CACHE_DIR_PFX}
    ...  ${CACHE_DIR_SFX}
    ...  ${CACHE_NUM_DISKS}
    ...  expectedCacheSize=1

    [Teardown]  Cache test teardown  ${DATADIR}

Test async caching with request 1 file date after request 2
    [Documentation]  Verify that later read requests for the same file with an earlier last-modified date are handled correctly.
    [Tags]           async
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

    ${testFile} =  Generate single test file  ${DATADIR}${/}testFile1  ${FILE_LENGTH}

    ${readRequestJan2} =  Make read request
    ...  ${testFile}
    ...  ${START_BLOCK}
    ...  ${END_BLOCK}
    ...  ${FILE_LENGTH}
    ...  ${LAST_MODIFIED_JAN_2_2018}
    ...  ${CLUSTER_TYPE}
    ${readRequestJan1} =  Make read request
    ...  ${testFile}
    ...  ${START_BLOCK}
    ...  ${END_BLOCK}
    ...  ${FILE_LENGTH}
    ...  ${LAST_MODIFIED_JAN_1_2018}
    ...  ${CLUSTER_TYPE}
    @{requests} =  CREATE LIST  ${readRequestJan2}  ${readRequestJan1}

    RUN KEYWORD IF  ${runConcurrently}
    ...  Execute concurrent requests
    ...  ${executionKeyword}
    ...  ${NUM_CONCURRENT_THREADS}
    ...  ${requests}
    ...  staggerRequests=${true}
    ...  ELSE
    ...  Execute sequential requests
    ...  ${executionKeyword}
    ...  ${requests}

    ${expectedProcessedRequests} =  SET VARIABLE  1
    ${totalRequests} =  GET LENGTH  ${requests}
    Verify metric value  ${METRIC_ASYNC_QUEUE_SIZE}  ${totalRequests}

    ${waitTime} =  EVALUATE  ${ASYNC_PROCESS_INTERVAL} * ${totalRequests} * 6
    SLEEP  ${waitTime}ms  Wait for queued requests to finish

    Verify async metrics
    ...  queueSize=0
    ...  processedRequests=${expectedProcessedRequests}
    ...  totalRequests=${totalRequests}
    ...  downloadedMB=${expectedProcessedRequests}
    Verify cache directory size
    ...  ${CACHE_DIR_PFX}
    ...  ${CACHE_DIR_SFX}
    ...  ${CACHE_NUM_DISKS}
    ...  expectedCacheSize=1

    [Teardown]  Cache test teardown  ${DATADIR}

Verify async metrics
    [Arguments]  ${queueSize}
    ...          ${processedRequests}
    ...          ${totalRequests}
    ...          ${downloadedMB}
    Verify metric value  ${METRIC_ASYNC_QUEUE_SIZE}  ${queueSize}
    Verify metric value  ${METRIC_ASYNC_PROCESSED_REQUESTS}  ${processedRequests}
    Verify metric value  ${METRIC_ASYNC_TOTAL_REQUESTS}  ${totalRequests}
    Verify metric value  ${METRIC_ASYNC_DOWNLOADED_MB}  ${downloadedMB}