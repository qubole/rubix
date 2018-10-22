*** Settings ***
Library     Collections
Library     OperatingSystem
Library     Process
Library     com.qubole.rubix.client.RubixClientLibrary

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
