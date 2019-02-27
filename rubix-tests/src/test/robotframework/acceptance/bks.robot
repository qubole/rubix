*** Settings ***
Library     Collections
Library     OperatingSystem
Library     Process
Library     com.qubole.rubix.client.robotframework.BookKeeperClientRFLibrary

*** Keywords ***

## Test Setup ##

Start BKS
    [Arguments]         &{options}
    ${optionArgs} =     Get options argument    &{options}
    Run                 ${CURDIR}${/}bks.sh start ${optionArgs}
    Sleep               1s

Stop BKS
    [Arguments]         &{options}
    ${optionArgs} =     Get options argument   &{options}
    Run                 ${CURDIR}${/}bks.sh stop ${optionArgs}

Get options argument
    [Arguments]         &{options}
    @{optionsList} =    Create List
    :FOR    ${key}      IN      @{options.keys()}
    \   ${value} =      Get From Dictionary     ${options}    ${key}
    \   Append To List  ${optionsList}  -D${key}\=${value}
    ${optionArgs} =     Join Command Line   ${optionsList}
    [Return]            ${optionArgs}

Create cache parent directories
    [Arguments]         ${cachePrefix}
    ...                 ${cacheNumDisks}
    :FOR    ${index}    IN RANGE    ${cacheNumDisks}
    \   Create directory    ${cachePrefix}${index}

Remove cache parent directories
    [Arguments]         ${cachePrefix}
    ...                 ${cacheNumDisks}
    :FOR    ${index}    IN RANGE    ${cacheNumDisks}
    \   Remove directory    ${cachePrefix}${index}  recursive=${True}

## Execution ##

Generate test files
    [Arguments]     ${numFiles}
    ...             ${fileLength}
    @{testFileList} =   Create List
    :FOR    ${index}    IN RANGE    ${numFiles}
    \   ${testFile} =        Set variable  ${REMOTE_PATH}${index}
    \   Generate test file   ${testFile}   ${fileLength}
    \   Append To List       ${testFileList}    ${testFile}
    [Return]        @{testFileList}

Make read request
    [Arguments]     ${file}
    ...             ${startBlock}
    ...             ${endBlock}
    ...             ${fileLength}
    ...             ${lastModified}
    ...             ${clusterType}
    ${request} =    Create test client read request
    ...             file://${file}
    ...             ${startBlock}
    ...             ${endBlock}
    ...             ${fileLength}
    ...             ${lastModified}
    ...             ${clusterType}
    [Return]        ${request}

Make read requests
    [Arguments]         ${files}
    ...                 ${startBlock}
    ...                 ${endBlock}
    ...                 ${fileLength}
    ...                 ${lastModified}
    ...                 ${clusterType}
    @{requests} =       Create List
    Log         Size of incoming files is ${files}
    :FOR    ${file}     IN      @{files}
    \   ${request} =    Create test client read request
    ...                 file://${file}
    ...                 ${startBlock}
    ...                 ${endBlock}
    ...                 ${fileLength}
    ...                 ${lastModified}
    ...                 ${clusterType}
    \   Append To List  ${requests}    ${request}
    [Return]            @{requests}

Download test file data to cache
    [Arguments]         ${file}
    ...                 ${startBlock}
    ...                 ${endBlock}
    ...                 ${fileLength}
    ...                 ${lastModified}
    ...                 ${clusterType}
    ${readRequest} =    Make read request
    ...                 ${file}
    ...                 ${startBlock}
    ...                 ${endBlock}
    ...                 ${fileLength}
    ...                 ${lastModified}
    ...                 ${clusterType}
    ${didRead} =        Download data to cache    ${readRequest}
    Should be true  ${didRead}

Multi download test file data to cache
    [Arguments]         ${numThreads}
    ...                 ${testFiles}
    ...                 ${START_BLOCK}
    ...                 ${END_BLOCK}
    ...                 ${FILE_LENGTH}
    ...                 ${LAST_MODIFIED}
    ...                 ${CLUSTER_TYPE}
    ${readRequests} =   Make read requests
    ...                 ${testFiles}
    ...                 ${START_BLOCK}
    ...                 ${END_BLOCK}
    ...                 ${FILE_LENGTH}
    ...                 ${LAST_MODIFIED}
    ...                 ${CLUSTER_TYPE}
    ${didReadAll} =     Multi download data to cache    ${numThreads}   ${readRequests}
    Should be true      ${didReadAll}

Read test file data
    [Arguments]         ${file}
    ...                 ${startBlock}
    ...                 ${endBlock}
    ...                 ${fileLength}
    ...                 ${lastModified}
    ...                 ${clusterType}
    ${readRequest} =    Make read request
    ...                 ${file}
    ...                 ${startBlock}
    ...                 ${endBlock}
    ...                 ${fileLength}
    ...                 ${lastModified}
    ...                 ${clusterType}
    ${didRead} =    Read data   ${readRequest}
    Should be true  ${didRead}

Multi read test file data
    [Arguments]         ${numThreads}
    ...                 ${testFiles}
    ...                 ${START_BLOCK}
    ...                 ${END_BLOCK}
    ...                 ${FILE_LENGTH}
    ...                 ${LAST_MODIFIED}
    ...                 ${CLUSTER_TYPE}
    ${readRequests} =   Make read requests
    ...                 ${testFiles}
    ...                 ${START_BLOCK}
    ...                 ${END_BLOCK}
    ...                 ${FILE_LENGTH}
    ...                 ${LAST_MODIFIED}
    ...                 ${CLUSTER_TYPE}
    ${didReadAll} =   Multi read data     ${numThreads}   ${readRequests}
    Should be true    ${didReadAll}

## Verification ##

Verify cache directory size
    [Arguments]     ${cachePrefix}
    ...             ${cacheSuffix}
    ...             ${cacheNumDisks}
    ...             ${expectedCacheSize}
    ${cacheDirSize} =               Get cache dir size MB   ${cachePrefix}   ${cacheSuffix}    ${cacheNumDisks}
    Should be equal as integers     ${cacheDirSize}         ${expectedCacheSize}

Verify metric value
    [Arguments]                 ${metricName}   ${expectedValue}
    &{metrics} =                Get cache metrics
    Should not be empty         ${metrics}
    Should be equal as numbers  &{metrics}[${metricName}]   ${expectedValue}
