*** Settings ***
Library     Collections
Library     OperatingSystem
Library     Process

*** Keywords ***

## Suite Setup/Teardown ##

Create cache parent directories
    [Arguments]  ${cachePrefix}  ${cacheNumDisks}
    :FOR  ${index}  IN RANGE  ${cacheNumDisks}
    \  CREATE DIRECTORY  ${cachePrefix}${index}

Remove cache parent directories
    [Arguments]  ${cachePrefix}  ${cacheNumDisks}
    :FOR  ${index}  IN RANGE  ${cacheNumDisks}
    \  REMOVE DIRECTORY  ${cachePrefix}${index}  recursive=${True}

## Test Setup/Teardown ##

Cache test setup
    [Arguments]   ${dataDir}  &{options}
    SET TEST VARIABLE  &{bksOptions}  &{options}
    CREATE DIRECTORY  ${dataDir}
    Start BKS  &{bksOptions}
    initialize Library Configuration  &{bksOptions}

Cache test teardown
    [Arguments]  ${dataDir}
    Stop BKS  &{bksOptions}
    REMOVE DIRECTORY  ${dataDir}  recursive=${True}

Start BKS
    [Arguments]  &{options}
    ${optionArgs} =  Get options argument  &{options}
    RUN  ${CURDIR}${/}bks.sh start-bks ${optionArgs}
    SLEEP  1s

Stop BKS
    [Arguments]  &{options}
    ${optionArgs} =  Get options argument  &{options}
    RUN  ${CURDIR}${/}bks.sh stop-bks ${optionArgs}

Start RubiX cluster
    ${output} =  RUN  ${CURDIR}${/}bks.sh start-cluster
    LOG  ${output}
    SLEEP  5s

Stop RubiX cluster
    ${output} =  RUN  ${CURDIR}${/}bks.sh stop-cluster
    LOG  ${output}

Get options argument
    [Arguments]  &{options}
    @{optionsList} =  CREATE LIST
    :FOR  ${key}  IN  @{options.keys()}
    \  ${value} =  GET FROM DICTIONARY  ${options}  ${key}
    \  APPEND TO LIST  ${optionsList}  -D${key}\=${value}
    ${optionArgs} =  JOIN COMMAND LINE  ${optionsList}
    [Return]  ${optionArgs}
