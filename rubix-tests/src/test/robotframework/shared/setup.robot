*** Settings ***
Library  Collections
Library  OperatingSystem
Library  Process

*** Keywords ***

## Suite Setup/Teardown ##

Create cache parent directories
    [Documentation]  Create parent directories needed for cache directory creation.
    [Arguments]  ${cachePrefix}  ${cacheNumDisks}
    :FOR  ${index}  IN RANGE  ${cacheNumDisks}
    \  CREATE DIRECTORY  ${cachePrefix}${index}

Remove cache parent directories
    [Documentation]  Remove all cache directories and their parents.
    [Arguments]  ${cachePrefix}  ${cacheNumDisks}
    :FOR  ${index}  IN RANGE  ${cacheNumDisks}
    \  REMOVE DIRECTORY  ${cachePrefix}${index}  recursive=${True}

## Test Setup/Teardown ##

Cache test setup
    [Documentation]  Performs steps necessary for setting up a test case.
    [Arguments]  ${dataDir}  &{options}
    SET TEST VARIABLE  &{bksOptions}  &{options}
    CREATE DIRECTORY  ${dataDir}
    Start BKS  &{bksOptions}
    initialize Library Configuration  &{bksOptions}

Cache test teardown
    [Documentation]  Performs steps necessary for tearing down a test case.
    [Arguments]  ${dataDir}
    Stop BKS  &{bksOptions}
    REMOVE DIRECTORY  ${dataDir}  recursive=${True}

Start BKS
    [Documentation]  Starts a BookKeeper server with the supplied options.
    [Arguments]  &{options}
    ${optionArgs} =  Get options argument  &{options}
    RUN  ${CURDIR}${/}bks.sh start-bks ${optionArgs}
    SLEEP  1s

Stop BKS
    [Documentation]  Shuts down the BookKeeper server used for the test.
    [Arguments]  &{options}
    ${optionArgs} =  Get options argument  &{options}
    RUN  ${CURDIR}${/}bks.sh stop-bks ${optionArgs}

Start RubiX cluster
    ${output} =  RUN  ${CURDIR}${/}bks.sh start-cluster
    LOG  ${output}
    SLEEP  12s  Allow time for daemons to start on cluster

Stop RubiX cluster
    ${output} =  RUN  ${CURDIR}${/}bks.sh stop-cluster
    LOG  ${output}

Get options argument
    [Documentation]  Get an argument string for configuration options to be specified when starting the BookKeeper server.
    [Arguments]  &{options}
    @{optionsList} =  CREATE LIST
    :FOR  ${key}  IN  @{options.keys()}
    \  ${value} =  GET FROM DICTIONARY  ${options}  ${key}
    \  APPEND TO LIST  ${optionsList}  -D${key}\=${value}
    ${optionArgs} =  JOIN COMMAND LINE  ${optionsList}
    [Return]  ${optionArgs}
