*** Settings ***
Library     Collections
Library     com.qubole.rubix.client.robotframework.BookKeeperClientRFLibrary

*** Keywords ***

## Generation ##

Generate single test file
    [Arguments]  ${fileName}  ${fileLength}
    generate Test File  ${fileName}  ${fileLength}
    [Return]  ${fileName}

Generate test files
    [Arguments]  ${fileName}
    ...          ${fileLength}
    ...          ${numFiles}
    ...          ${offset}=0
    @{testFileList} =  CREATE LIST
    :FOR  ${index}  IN RANGE  ${offset}  ${numFiles}
    \  ${testFile} =  SET VARIABLE  ${fileName}${index}
    \  generate Test File  ${testFile}  ${fileLength}
    \  APPEND TO LIST  ${testFileList}  ${testFile}
    [Return]  @{testFileList}

Make read request
    [Arguments]  ${fileName}
    ...          ${startBlock}
    ...          ${endBlock}
    ...          ${fileLength}
    ...          ${lastModified}
    ${request} =  create Test Client Read Request
    ...  file://${fileName}
    ...  ${startBlock}
    ...  ${endBlock}
    ...  ${fileLength}
    ...  ${lastModified}
    [Return]  ${request}

Make similar read requests
    [Arguments]  ${fileNames}
    ...          ${startBlock}
    ...          ${endBlock}
    ...          ${fileLength}
    ...          ${lastModified}
    @{requests} =  CREATE LIST
    :FOR  ${fileName}  IN  @{fileNames}
    \   ${request} =  create Test Client Read Request
    ...  file://${fileName}
    ...  ${startBlock}
    ...  ${endBlock}
    ...  ${fileLength}
    ...  ${lastModified}
    \   APPEND TO LIST  ${requests}    ${request}
    [Return]  @{requests}

## Execution ##

Execute concurrent requests
    [Arguments]  ${executionKeyword}  ${numThreads}  ${requests}
    RUN KEYWORD  ${executionKeyword}  ${numThreads}  ${requests}

Execute sequential requests
    [Arguments]  ${executionKeyword}
    ...          ${requests}
    :FOR  ${request}  IN  @{requests}
    \  RUN KEYWORD  ${executionKeyword}  ${request}

Download requests
    [Arguments]  ${readRequest}
    ${didRead} =  download Data To Cache  ${readRequest}
    SHOULD BE TRUE  ${didRead}

Concurrently download requests
    [Arguments]  ${numThreads}  ${readRequests}
    ${didReadAll} =  concurrent Download Data To Cache  ${numThreads}  @{readRequests}
    SHOULD BE TRUE  ${didReadAll}

Read requests
    [Arguments]  ${readRequest}
    ${didRead} =  read Data  ${readRequest}
    SHOULD BE TRUE  ${didRead}

Concurrently read requests
    [Arguments]  ${numThreads}  ${readRequests}
    ${didReadAll} =  concurrent Read Data  ${numThreads}  @{readRequests}
    SHOULD BE TRUE  ${didReadAll}

## Verification ##

Verify cache directory size
    [Arguments]  ${cachePrefix}
    ...          ${cacheSuffix}
    ...          ${cacheNumDisks}
    ...          ${expectedCacheSize}
    ${cacheDirSize} =  get Cache Dir Size MB  ${cachePrefix}  ${cacheSuffix}  ${cacheNumDisks}
    SHOULD BE EQUAL AS INTEGERS  ${cacheDirSize}  ${expectedCacheSize}

Verify metric value
    [Arguments]  ${metricName}  ${expectedValue}
    &{metrics} =  get Cache Metrics
    LOG MANY  &{metrics}
    SHOULD NOT BE EMPTY  ${metrics}
    SHOULD BE EQUAL AS NUMBERS  &{metrics}[${metricName}]  ${expectedValue}
