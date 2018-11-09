*** Settings ***
Library     Collections
Library     OperatingSystem
Library     Process
Library     com.qubole.rubix.client.robotframework.BookKeeperClientRFLibrary

*** Keywords ***
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

Generate test files
    [Arguments]     ${numFiles}
    ...             ${fileLength}
    @{testFileList} =   Create List
    :FOR    ${index}    IN RANGE    ${numFiles}
    \   ${testFile} =        Set variable  ${REMOTE_PATH}${index}
    \   Generate test file   ${testFile}   ${fileLength}
    \   Append To List       ${testFileList}    ${testFile}
    [Return]        @{testFileList}

Read test file data using API
    [Arguments]     ${fileName}  ${startBlock}  ${endBlock}  ${fileLength}  ${lastModified}  ${clusterType}
    ${didRead} =    Read data using client api
    ...             file://${fileName}
    ...             ${startBlock}
    ...             ${endBlock}
    ...             ${fileLength}
    ...             ${lastModified}
    ...             ${clusterType}
    Should be true  ${didRead}

Read test file data using FS
    [Arguments]     ${fileName}  ${startBlock}  ${endBlock}
    ${didRead} =    Read data using file system
    ...             ${fileName}
    ...             ${startBlock}
    ...             ${endBlock}
    Should be true  ${didRead}

Verify cache directories
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
