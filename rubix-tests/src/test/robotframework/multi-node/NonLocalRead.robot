*** Settings ***
Documentation   RubiX Multi-Node Integration Tests
Resource        ..${/}shared${/}setup.robot
Resource        ..${/}shared${/}bookkeeper.robot
Library         com.qubole.rubix.client.robotframework.container.client.ContainerRequestClient
Library         com.qubole.rubix.client.robotframework.testdriver.WorkerTestDriver

*** Variables ***
# Cache settings
${WORKINGDIR}  ${/}tmp${/}rubix${/}tests${/}NonLocalRead
${DATADIR}     ${WORKINGDIR}${/}data${/}

${HOSTNAME_MASTER}   172.18.8.0
${HOSTNAME_WORKER1}  172.18.8.1
${HOSTNAME_WORKER2}  172.18.8.2

# Request specs
${NUM_TEST_FILES}  2

${FILEPREFIX}  ${DATADIR}${/}testFile
${TEST_FILE_1}  ${DATADIR}${/}testFile0
${TEST_FILE_2}  ${DATADIR}${/}testFile1

${REMOTE_PATH}      ${DATADIR}${/}rubixIntegrationTestFile
${FILE_LENGTH}    1048576
${LAST_MODIFIED}  1514764800
${START_BLOCK}    0
${END_BLOCK}      1048576
${CLUSTER_TYPE}   3   # TEST_CLUSTER_MANAGER

${METRIC_NONLOCAL_REQUESTS}  rubix.bookkeeper.count.nonlocal_request
${METRIC_REMOTE_REQUESTS}    rubix.bookkeeper.count.remote_request

${NUM_EXPECTED_REQUESTS_NONLOCAL}  1
${NUM_EXPECTED_REQUESTS_REMOTE}    1

*** Test Cases ***
Templated driver test case
    [Template]  Test coordinator driver
    [Tags]  driver
    #       R  C  NL
    3  100  1  0  0     # all remote
    3  100  0  1  0     # all cached
    3  100  0  0  1     # all non-local
    3  100  1  1  0     # 1/2 remote, 1/2 cached
    3  100  1  0  1     # 1/2 remote, 1/2 non-local
    3  100  0  1  1     # 1/2 cached, 1/2 non-local
    3  99  1  1  1      # even split

Testing Worker Test Driver
    [Tags]  workertestdriver
    # [Setup]
    Multi-node test setup  ${DATADIR}

    ${fileName} =  Generate single test file  ${REMOTE_PATH}  ${FILE_LENGTH}

    ${Taskobject} =  create Task
    ...  ${REMOTE_PATH}
    ...  ${START_BLOCK}
    ...  ${END_BLOCK}
    ...  ${FILE_LENGTH}
    ...  ${LAST_MODIFIED}
    ...  ${CLUSTER_TYPE}

    ${verified} =  execute Task  ${Taskobject}

    SHOULD BE TRUE  ${verified}

    [Teardown]  Multi-node test teardown  ${DATADIR}

*** Keywords ***
Test coordinator driver
    [Arguments]  ${numWorkers}
    ...          ${numTasks}
    ...          ${remoteRatio}
    ...          ${cacheRatio}
    ...          ${nonlocalRatio}

    # [Setup]
    Multi-node test setup  ${DATADIR}

    @{fileNames} =  Generate test files  ${FILEPREFIX}  ${FILE_LENGTH}  ${numTasks}

    ${job} =  make Job
    ...  ${remoteRatio}  ${cacheRatio}  ${nonlocalRatio}
    ...  ${fileNames}
    ...  ${START_BLOCK}
    ...  ${END_BLOCK}
    ...  ${FILE_LENGTH}
    ...  ${LAST_MODIFIED}
    ...  ${CLUSTER_TYPE}

    ${didRun} =  run Rubix Job  ${HOSTNAME_MASTER}  ${job}
    SHOULD BE TRUE  ${didRun}

    ${verified} =  verify Rubix Job  ${HOSTNAME_MASTER}  ${job}
    SHOULD BE TRUE  ${verified}

    [Teardown]  Multi-node test teardown  ${DATADIR}
