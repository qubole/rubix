*** Settings ***
Library     Collections
Library     OperatingSystem
Library     Process

*** Keywords ***

## Suite Setup/Teardown ##

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

## Test Setup/Teardown ##

Cache test setup
    [Arguments]                         ${dataDir}      &{options}
    Set Test Variable                   &{bksOptions}   &{options}
    Create Directory                    ${dataDir}
    Start BKS                           &{bksOptions}
    Initialize Library Configuration    &{bksOptions}

Cache test teardown
    [Arguments]         ${dataDir}
    Stop BKS            &{bksOptions}
    Remove Directory    ${dataDir}      recursive=${True}

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
