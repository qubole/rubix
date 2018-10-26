*** Settings ***
Library     Collections
Library     OperatingSystem
Library     Process
Library     com.qubole.rubix.client.BookKeeperClientRFLibrary

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

Generate and cache test files
    [Arguments]     ${numFiles}
    ...             ${startBlock}
    ...             ${endBlock}
    ...             ${fileLength}
    ...             ${lastModified}
    ...             ${clusterType}
    :FOR    ${index}    IN RANGE    ${numFiles}
    \  ${testFile} =        Set variable  ${REMOTE_PATH}${index}
    \  Generate test file   ${testFile}   ${fileLength}
    \  Read test file data  ${testFile}   ${startBlock}   ${endBlock}   ${fileLength}   ${lastModified}   ${clusterType}

Read test file data
    [Arguments]     ${fileName}  ${startBlock}  ${endBlock}  ${fileLength}  ${lastModified}  ${clusterType}
    ${didRead} =    Read data
    ...             file://${fileName}
    ...             ${startBlock}
    ...             ${endBlock}
    ...             ${fileLength}
    ...             ${lastModified}
    ...             ${clusterType}
    Should be true  ${didRead}

Get metric value
    [Arguments]             ${metricName}
    &{metrics} =            Get cache metrics
    Should not be empty     ${metrics}
    [Return]                &{metrics}[${metricName}]
