*** Settings ***
Documentation       Rubix Asynchronous Download Integration Tests
Resource            bks.robot
Suite Setup         Create Cache Parent Directories     ${CACHE_DIR_PFX}    ${CACHE_NUM_DISKS}
Suite Teardown      Remove Cache Parent Directories     ${CACHE_DIR_PFX}    ${CACHE_NUM_DISKS}

*** Variables ***
${WORKINGDIR}       ${TEMPDIR}${/}AsyncWarmup
${DATADIR}          ${WORKINGDIR}${/}data

${CACHE_DIR_PFX}    ${WORKINGDIR}${/}
${CACHE_DIR_SFX}    /fcache/
${CACHE_NUM_DISKS}  1

${METRIC_ASYNC_QUEUE_SIZE}          rubix.bookkeeper.gauge.async_queue_size
${METRIC_ASYNC_PROCESSED_REQUESTS}  rubix.bookkeeper.count.processed_async_request
${METRIC_ASYNC_TOTAL_REQUESTS}      rubix.bookkeeper.count.total_async_request
${METRIC_ASYNC_DOWNLOADED_MB}       rubix.bookkeeper.count.async_downloaded_mb

${REMOTE_PATH}      ${DATADIR}${/}rubixIntegrationTestFile
${FILE_LENGTH}      1048576
${LAST_MODIFIED}    1514764800
${START_BLOCK}      0
${END_BLOCK}        1048576
${CLUSTER_TYPE}     3   # TEST_CLUSTER_MANAGER

${NUM_TEST_FILES}           5
${ASYNC_PROCESS_INTERVAL}   1000

${ASYNC_FETCH_DELAY_PROCESS_INTERVAL}   10000
${ASYNC_FETCH_DELAY}                    5000

*** Test Cases ***
Test async caching when data downloaded
    [Documentation]     Using the BKS Thrift API, verify that files are correctly cached when asynchronously downloaded.
    [Tags]              async
    [Setup]             Cache test setup
    ...                 ${DATADIR}
    ...                 rubix.cluster.on-master=true
    ...                 hadoop.cache.data.dirprefix.list=${CACHE_DIR_PFX}
    ...                 hadoop.cache.data.dirsuffix=${CACHE_DIR_SFX}
    ...                 hadoop.cache.data.max.disks=${CACHE_NUM_DISKS}
    ...                 rubix.parallel.warmup=true
    ...                 rubix.request.process.initial.delay=${ASYNC_PROCESS_INTERVAL}
    ...                 rubix.request.process.interval=${ASYNC_PROCESS_INTERVAL}
    ...                 rubix.remotefetch.interval=${ASYNC_PROCESS_INTERVAL}
    [Teardown]          Cache test teardown     ${DATADIR}

    @{testFiles} =      Generate test files   ${REMOTE_PATH}    ${NUM_TEST_FILES}   ${FILE_LENGTH}
    :FOR    ${file}     IN      @{testFiles}
    \    Download test file data to cache   ${file}     ${START_BLOCK}   ${END_BLOCK}   ${FILE_LENGTH}   ${LAST_MODIFIED}   ${CLUSTER_TYPE}

    Verify metric value             ${METRIC_ASYNC_QUEUE_SIZE}  ${NUM_TEST_FILES}

    ${waitTime} =       Evaluate    ${ASYNC_PROCESS_INTERVAL} * ${NUM_TEST_FILES}
    Sleep               ${waitTime}ms      Wait for queued requests to finish

    Verify async metrics    0   ${NUM_TEST_FILES}   ${NUM_TEST_FILES}   ${NUM_TEST_FILES}
    Verify cache directory size     ${CACHE_DIR_PFX}    ${CACHE_DIR_SFX}    ${CACHE_NUM_DISKS}    ${NUM_TEST_FILES}

Test async caching when data read
    [Documentation]     Using a caching FS, verify that files are correctly cached when asynchronously downloaded.
    [Tags]              async
    [Setup]             Cache test setup
    ...                 ${DATADIR}
    ...                 rubix.cluster.on-master=true
    ...                 hadoop.cache.data.dirprefix.list=${CACHE_DIR_PFX}
    ...                 hadoop.cache.data.dirsuffix=${CACHE_DIR_SFX}
    ...                 hadoop.cache.data.max.disks=${CACHE_NUM_DISKS}
    ...                 rubix.parallel.warmup=true
    ...                 rubix.request.process.initial.delay=${ASYNC_PROCESS_INTERVAL}
    ...                 rubix.request.process.interval=${ASYNC_PROCESS_INTERVAL}
    ...                 rubix.remotefetch.interval=${ASYNC_PROCESS_INTERVAL}
    [Teardown]          Cache test teardown     ${DATADIR}

    @{testFiles} =      Generate test files   ${REMOTE_PATH}    ${NUM_TEST_FILES}   ${FILE_LENGTH}
    :FOR    ${file}     IN      @{testFiles}
    \    Read test file data   ${file}     ${START_BLOCK}   ${END_BLOCK}

    Verify metric value             ${METRIC_ASYNC_QUEUE_SIZE}  ${NUM_TEST_FILES}

    ${waitTime} =       Evaluate    ${ASYNC_PROCESS_INTERVAL} * ${NUM_TEST_FILES}
    Sleep               ${waitTime}ms      Wait for queued requests to finish

    Verify async metrics    0   ${NUM_TEST_FILES}   ${NUM_TEST_FILES}   ${NUM_TEST_FILES}
    Verify cache directory size     ${CACHE_DIR_PFX}    ${CACHE_DIR_SFX}    ${CACHE_NUM_DISKS}    ${NUM_TEST_FILES}

Test async caching fetch delay when data downloaded
    [Documentation]     Using the BKS Thrift API, verify that asynchronous fetching only downloads files queued outside of the delay period.
    [Tags]              async
    [Setup]             Cache test setup
    ...                 ${DATADIR}
    ...                 rubix.cluster.on-master=true
    ...                 hadoop.cache.data.dirprefix.list=${CACHE_DIR_PFX}
    ...                 hadoop.cache.data.dirsuffix=${CACHE_DIR_SFX}
    ...                 hadoop.cache.data.max.disks=${CACHE_NUM_DISKS}
    ...                 rubix.parallel.warmup=true
    ...                 rubix.request.process.initial.delay=${ASYNC_FETCH_DELAY_PROCESS_INTERVAL}
    ...                 rubix.request.process.interval=${ASYNC_FETCH_DELAY_PROCESS_INTERVAL}
    ...                 rubix.remotefetch.interval=${ASYNC_FETCH_DELAY}
    [Teardown]          Cache test teardown     ${DATADIR}

    ${numFilesFirstPass} =      Set variable    3
    ${numFilesSecondPass} =     Set variable    2
    ${numTotalFiles} =          Evaluate        ${numFilesFirstPass} + ${numFilesSecondPass}

    @{testFilesFirstPass} =     Generate test files   ${DATADIR}${/}firstPass    ${numFilesFirstPass}   ${FILE_LENGTH}
    :FOR    ${file}     IN      @{testFilesFirstPass}
    \    Download test file data to cache   ${file}     ${START_BLOCK}   ${END_BLOCK}   ${FILE_LENGTH}   ${LAST_MODIFIED}   ${CLUSTER_TYPE}

    Verify async metrics    ${numFilesFirstPass}
    ...                     0
    ...                     ${numFilesFirstPass}
    ...                     0

    Sleep                   6000ms      Hold for second set so next files are postponed due to process delay

    @{testFilesSecondPass} =    Generate test files   ${DATADIR}${/}secondPass    ${numFilesSecondPass}   ${FILE_LENGTH}
    :FOR    ${file}     IN      @{testFilesSecondPass}
    \    Download test file data to cache   ${file}     ${START_BLOCK}   ${END_BLOCK}   ${FILE_LENGTH}   ${LAST_MODIFIED}   ${CLUSTER_TYPE}

    Verify async metrics    ${numTotalFiles}
    ...                     0
    ...                     ${numTotalFiles}
    ...                     0

    Sleep                   5000ms      Wait for first-pass files to process

    Verify async metrics    ${numFilesSecondPass}
    ...                     ${numFilesFirstPass}
    ...                     ${numTotalFiles}
    ...                     ${numFilesFirstPass}

    Sleep                   10000ms     Wait for remaining files to process

    Verify async metrics    0
    ...                     ${numTotalFiles}
    ...                     ${numTotalFiles}
    ...                     ${numTotalFiles}
    Verify cache directory size     ${CACHE_DIR_PFX}    ${CACHE_DIR_SFX}    ${CACHE_NUM_DISKS}    ${numTotalFiles}

Test async caching remote fetch interval when data read
    [Documentation]     Using a caching FS, verify that asynchronous fetching only downloads files queued outside of the delay period.
    [Tags]              async
    [Setup]             Cache test setup
    ...                 ${DATADIR}
    ...                 rubix.cluster.on-master=true
    ...                 hadoop.cache.data.dirprefix.list=${CACHE_DIR_PFX}
    ...                 hadoop.cache.data.dirsuffix=${CACHE_DIR_SFX}
    ...                 hadoop.cache.data.max.disks=${CACHE_NUM_DISKS}
    ...                 rubix.parallel.warmup=true
    ...                 rubix.request.process.initial.delay=${ASYNC_FETCH_DELAY_PROCESS_INTERVAL}
    ...                 rubix.request.process.interval=${ASYNC_FETCH_DELAY_PROCESS_INTERVAL}
    ...                 rubix.remotefetch.interval=${ASYNC_FETCH_DELAY}
    [Teardown]          Cache test teardown     ${DATADIR}

    ${numFilesFirstPass} =      Set variable    3
    ${numFilesSecondPass} =     Set variable    2
    ${numTotalFiles} =          Evaluate        ${numFilesFirstPass} + ${numFilesSecondPass}

    @{testFilesFirstPass} =     Generate test files   ${DATADIR}${/}firstPass    ${numFilesFirstPass}   ${FILE_LENGTH}
    :FOR    ${file}     IN      @{testFilesFirstPass}
    \    Read test file data   ${file}     ${START_BLOCK}   ${END_BLOCK}

    Verify async metrics    ${numFilesFirstPass}
    ...                     0
    ...                     ${numFilesFirstPass}
    ...                     0

    Sleep                   6000ms      Hold for second set so next files are postponed due to process delay

    @{testFilesSecondPass} =    Generate test files   ${DATADIR}${/}secondPass    ${numFilesSecondPass}   ${FILE_LENGTH}
    :FOR    ${file}     IN      @{testFilesSecondPass}
    \    Read test file data   ${file}     ${START_BLOCK}   ${END_BLOCK}

    Verify async metrics    ${numTotalFiles}
    ...                     0
    ...                     ${numTotalFiles}
    ...                     0

    Sleep                   5000ms      Wait for first-pass files to process

    Verify async metrics    ${numFilesSecondPass}
    ...                     ${numFilesFirstPass}
    ...                     ${numTotalFiles}
    ...                     ${numFilesFirstPass}

    Sleep                   10000ms     Wait for remaining files to process

    Verify async metrics    0
    ...                     ${numTotalFiles}
    ...                     ${numTotalFiles}
    ...                     ${numTotalFiles}
    Verify cache directory size     ${CACHE_DIR_PFX}    ${CACHE_DIR_SFX}    ${CACHE_NUM_DISKS}    ${numTotalFiles}

*** Keywords ***
Verify async metrics
    [Arguments]     ${queueSize}
    ...             ${processedRequests}
    ...             ${totalRequests}
    ...             ${downloadedMB}
    Verify metric value     ${METRIC_ASYNC_QUEUE_SIZE}          ${queueSize}
    Verify metric value     ${METRIC_ASYNC_PROCESSED_REQUESTS}  ${processedRequests}
    Verify metric value     ${METRIC_ASYNC_TOTAL_REQUESTS}      ${totalRequests}
    Verify metric value     ${METRIC_ASYNC_DOWNLOADED_MB}       ${downloadedMB}