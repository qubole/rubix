*** Settings ***
Library     Collections
Library     com.qubole.rubix.client.robotframework.BookKeeperClientRFLibrary

*** Keywords ***

## Generation ##

Generate single test file
    [Arguments]         ${fileName}
    ...                 ${fileLength}
    Generate test file  ${fileName}   ${fileLength}
    [Return]            ${fileName}

Generate test files
    [Arguments]     ${fileName}
    ...             ${fileLength}
    ...             ${numFiles}
    ...             ${offset}=0
    @{testFileList} =   Create List
    :FOR    ${index}    IN RANGE    ${offset}    ${numFiles}
    \   ${testFile} =        Set variable  ${fileName}${index}
    \   Generate test file   ${testFile}   ${fileLength}
    \   Append To List       ${testFileList}    ${testFile}
    [Return]        @{testFileList}

Make read request
    [Arguments]     ${fileName}
    ...             ${startBlock}
    ...             ${endBlock}
    ...             ${fileLength}
    ...             ${lastModified}
    ...             ${clusterType}
    ${request} =    Create test client read request
    ...             file://${fileName}
    ...             ${startBlock}
    ...             ${endBlock}
    ...             ${fileLength}
    ...             ${lastModified}
    ...             ${clusterType}
    [Return]        ${request}

Make similar read requests
    [Arguments]         ${fileNames}
    ...                 ${startBlock}
    ...                 ${endBlock}
    ...                 ${fileLength}
    ...                 ${lastModified}
    ...                 ${clusterType}
    @{requests} =       Create List
    :FOR    ${fileName}     IN      @{fileNames}
    \   ${request} =    Create test client read request
    ...                 file://${fileName}
    ...                 ${startBlock}
    ...                 ${endBlock}
    ...                 ${fileLength}
    ...                 ${lastModified}
    ...                 ${clusterType}
    \   Append To List  ${requests}    ${request}
    [Return]            @{requests}

## Execution ##

Execute concurrent requests
    [Arguments]         ${clientKeyword}
    ...                 ${numThreads}
    ...                 ${requests}
    Run Keyword         ${clientKeyword}
    ...                 ${numThreads}
    ...                 ${requests}

Execute sequential requests
    [Arguments]         ${clientKeyword}
    ...                 ${requests}
    :FOR    ${request}     IN      @{requests}
    \    Run Keyword    ${clientKeyword}
    ...                 ${request}

Download requests
    [Arguments]         ${readRequest}
    ${didRead} =        Download data to cache    ${readRequest}
    Should be true      ${didRead}

Multi download requests
    [Arguments]         ${numThreads}
    ...                 ${readRequests}
    ${didReadAll} =     Multi download data to cache    ${numThreads}   @{readRequests}
    Should be true      ${didReadAll}

Read requests
    [Arguments]         ${readRequest}
    ${didRead} =        Read data    ${readRequest}
    Should be true      ${didRead}

Multi read requests
    [Arguments]         ${numThreads}
    ...                 ${readRequests}
    ${didReadAll} =     Multi read data    ${numThreads}    @{readRequests}
    Should be true      ${didReadAll}

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
    Log Many                    &{metrics}
    Should not be empty         ${metrics}
    Should be equal as numbers  &{metrics}[${metricName}]   ${expectedValue}
