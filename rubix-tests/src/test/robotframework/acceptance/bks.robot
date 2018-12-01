*** Settings ***
Library     Collections
Library     OperatingSystem
Library     Process
Library     com.qubole.rubix.client.robotframework.BookKeeperClientRFLibrary

*** Keywords ***

## Test Setup ##

Cache test setup
    [Arguments]                         ${dataDir}      &{options}
    Set Test Variable                   &{bksOptions}   &{options}
    Create Directory                    ${dataDir}
    Start BKS                           &{bksOptions}
    Initialize Library Configuration    &{bksOptions}

Cache test teardown
    [Arguments]                         ${dataDir}
    Stop BKS                            &{bksOptions}
    Remove Directory                    ${dataDir}      recursive=${True}

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
    [Arguments]     ${fileName}
    ...             ${numFiles}
    ...             ${fileLength}
    @{testFileList} =   Create List
    :FOR    ${index}    IN RANGE    ${numFiles}
    \   ${testFile} =        Set variable  ${fileName}${index}
    \   Generate test file   ${testFile}   ${fileLength}
    \   Append To List       ${testFileList}    ${testFile}
    [Return]        @{testFileList}

Download test file data to cache
    [Arguments]     ${fileName}  ${startBlock}  ${endBlock}  ${fileLength}  ${lastModified}  ${clusterType}
    ${didRead} =    Download data to cache
    ...             file://${fileName}
    ...             ${startBlock}
    ...             ${endBlock}
    ...             ${fileLength}
    ...             ${lastModified}
    ...             ${clusterType}
    Should be true  ${didRead}

Read test file data
    [Arguments]     ${fileName}  ${startBlock}  ${endBlock}
    ${didRead} =    Read data
    ...             file://${fileName}
    ...             ${startBlock}
    ...             ${endBlock}
    Should be true  ${didRead}

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
