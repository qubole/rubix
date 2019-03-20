# RubiX Integration Test Style Guide

## Test Cases

### Templates

[Test templates](http://robotframework.org/robotframework/latest/RobotFrameworkUserGuide.html#test-templates) 
are used to repeatedly run the same test keyword with different client scenarios
(such as directly using the Thrift API to cache data, or running multiple concurrent read calls using a CachingFileSystem).  

### Variables

Variables within a keyword are **camelCase**

    @{testFileNames} =  Generate test files  ${REMOTE_PATH}  ${FILE_LENGTH}  ${NUM_TEST_FILES}
    
Variables in the "Variables" section of a test case are **ALL_CAPS_AND_UNDERSCORE_SEPARATED**
(like Java constants).

    ${NUM_EXPECTED_EVICTIONS}  3


## Keywords

Keywords specific to test suites should be added to the same test suite file.

Keywords that are used by multiple test suites should be added to an appropriate resource file.
(eg. test setup keywords should be placed in `setup.robot`).

### Names

Built-in keywords are **ALL CAPS**.
    
    CREATE DIRECTORY  ${directoryName}

Test keywords (defined in a test or resource `.robot` file) are **Sentence capitalized**.

    Generate single test file  ${fileName}  ${fileLength}

Custom library keywords (eg. from BookKeeperRFClientLibrary) are **camel Case**.

    &{metrics} =  get Cache Metrics

### Arguments

Arguments have **2 spaces** between the keyword and each other.

    Verify metric value  ${METRIC_EVICTION}  ${NUM_EXPECTED_EVICTIONS}
  
If the keyword needs more than 3 arguments, place the arguments on separate lines.

    ${request} =  Create test client read request
    ...  ${fileName}
    ...  ${startBlock}
    ...  ${endBlock}
    ...  ${fileLength}
    ...  ${lastModified}
    ...  ${clusterType}

Use named arguments for keywords where possible to enhance clarity.

    Verify cache directory size
            .....
    ...  expectedCacheSize=${CACHE_MAX_SIZE}

For sets of keywords with similar arguments, alignment of arguments is preferred. 

    [Template]   Test cache eviction
    Download requests               runConcurrently=${false}
    Concurrently download requests  runConcurrently=${true}
    Read requests                   runConcurrently=${false}
    Concurrently read requests      runConcurrently=${true}
