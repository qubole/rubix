# Robot Framework Integration Tests

For more detailed info regarding Robot Framework and its capabilities, 
read the [Robot Framework user guide](http://robotframework.org/robotframework/latest/RobotFrameworkUserGuide.html).

### Quick Notes

* To run tests with a specific tag: `./mvnw ... -Dincludes=<comma-separated list of tags>`
* To run a specific test: `./mvnw ... -Dtests='<test name with spaces removed>'` (case-insensitive)
* Logs will be generated in `rubix-tests/src/target/integration-test-logs`.

## Test Suites

Each `.robot` file is a test suite. 
Related tests should be kept in the same test suite,
to allow for reuse of variables and suite-level keywords.

Test suites contain the following sections:

### *** Settings ***

This section is for specifying suite-level documentation,
as well as keywords for suite-level setup & teardown.

This section also specifies other sources for keywords used by the test suite.
These can be:

* Robot Framework built-in libraries, such as *Collections* and *OperatingSystem*
* Other `.robot` files containing helpful keywords, such as `setup.robot` for setting up & tearing down tests.
* A fully-qualified Java class from a custom library containing methods that can be run as keywords (more info [below](#custom-keywords)) 

Example:
```
*** Settings ***
Resource  OperatingSystem
Resource  bookkeeper.robot
Resource  com.qubole.rubix.client.robotframework.BookKeeperClientRFLibrary
```

### *** Variables ***

This section contains any variables common to the test cases in the suite.

Common variables needed across test suites include the following:

```
${WORKINGDIR}   ${TEMPDIR}${/}<test-suite-name>
${DATADIR}      ${WORKINGDIR}${/}data

${CACHE_DIR_PFX}    ${WORKINGDIR}${/}
${CACHE_DIR_SFX}    /fcache/
${CACHE_NUM_DISKS}  <number-of-cache-disks>
```

Note: `${TEMPDIR}` is supplied by Robot Framework at points to the operating system's temp directory,
while `${/}` is the operating system's path separator.
 
<!--  Maybe add something about built-in variables, like ${TEMPDIR}  -->

### *** Test Cases ***

This is where test cases are defined.

Test cases include a name on its own line, 
followed by the keywords to be executed for the test on indented lines following it.

For RubiX, we use [test templates](http://robotframework.org/robotframework/latest/RobotFrameworkUserGuide.html#test-templates)
to run the test and verify that it passes with different modes of execution.

### *** Keywords ***

This section contains any suite-level keywords, 
as well as keywords for running tests defined as templates.

Like test cases, keywords include their name on its own line, 
and the keywords to be run on indented lines after it. 

Keywords should include a **\[Documentation\]** tag to provide details
regarding the purpose and/or usage of the keyword.


## Test Cases

In general, a test case will require the following components:

* Setup
* Body
    * Data generation
    * Execution
    * Verification
* Teardown

### Setup

Start the test case with the **Cache test setup** keyword 
to start a BookKeeper server with the provided configuration options
and create the directory used for storing generated data for the test.

The following example starts a server as a master,
and configures the cache directory settings and its maximum size.
```
Cache test setup
...  ${DATADIR}
...  rubix.cluster.is-master=true
...  rubix.cache.dirprefix.list=${CACHE_DIR_PFX}
...  rubix.cache.dirsuffix=${CACHE_DIR_SFX}
...  rubix.cache.max.disks=${CACHE_NUM_DISKS}
...  rubix.cache.fullness.size=${CACHE_MAX_SIZE}
```

### Test Body

Integration tests need to:
 * generate any files needed for test execution
 * execute whatever steps necessary to sufficiently test the desired scenario
 * verify the state of the BookKeeper & cache using metrics and other helper keywords

#### Generation

You can generate data files individually:

    ${fileName} =  Generate single test file  ${filePath}  ${fileLength}

or as a batch of files with similar characteristics:

    @{fileNames} =  Generate test files  ${filePathPrefix}  ${fileLength}  ${numberOfFiles}

#### Execution

In order to execute calls using the BookKeeper server, you will need to make a request object.

Similar to generating test files, requests can be generated individually:

```
${request} =  Make read request
...  ${fileName}
...  ${startBlock}
...  ${endBlock}
...  ${fileLength}
...  ${lastModified}
...  ${clusterType}
```

or as a batch of requests with similar characteristics:

```
@{requests} =  Make similar read requests
...  ${fileNames}
...  ${startBlock}
...  ${endBlock}
...  ${fileLength}
...  ${lastModified}
...  ${clusterType}
```

For read calls, current execution modes include combinations of:

* Caching data by **directly calling the BookKeeper server** OR **using a client file system**
* Executing caching calls **sequentially** OR **concurrently**

The execution mode is determined by the keyword name passed into the test template.
For example, the template below will first run the test with sequential calls to the BookKeeper server,
and then with concurrent calls using the client file system.  

```
Cache eviction
    [Template]  Test cache eviction
    Execute read requests using BookKeeper server call           runConcurrently=${false}
    Concurrently execute read requests using client file system  runConcurrently=${true}
```

The actual execution of the keyword is controlled by the following step,
which will run the keyword concurrently on the specified number of threads if the flag is set to true,
or sequentially otherwise.

```
RUN KEYWORD IF  ${runConcurrently}
...  Execute concurrent requests
...  ${executionKeyword}
...  ${numThreads}
...  ${requests}
...  ELSE
...  Execute sequential requests
...  ${executionKeyword}
...  ${requests}
```

#### Verification 

Test execution can be verified by comparing metrics values to expected values.

    Verify metric value  ${metricName}  ${expectedValue}
    
As well, you can verify that the size of the cache is the expected size.

    Verify cache directory size
    ...  ${cacheDirPrefix}
    ...  ${cacheDirSuffix}
    ...  ${cacheDirNumDisks}
    ...  ${expectedCacheSize}

### Teardown

Finish the test case with the **Cache test teardown** keyword as a **\[Teardown\]** step; 
this ensures the BookKeeper server used for this test is properly shut down 
and the environment is cleaned before execution of the next test.

    [Teardown]  Cache test teardown  ${DATADIR}


## Style Guide

### Variables

Variables within a keyword are **camelCase**.

    @{testFileNames} =  Generate test files  ${REMOTE_PATH}  ${FILE_LENGTH}  ${NUM_TEST_FILES}
    
Variables in the "Variables" section of a test case are **ALL_CAPS_AND_UNDERSCORE_SEPARATED**
(like Java constants).

    ${NUM_EXPECTED_EVICTIONS}  3

### Keywords

#### Names

Built-in keywords are **ALL CAPS**.
    
    CREATE DIRECTORY  ${directoryName}

Test keywords (defined in a test or resource `.robot` file) are **Sentence capitalized**.

    Generate single test file  ${fileName}  ${fileLength}

Custom library keywords (eg. from BookKeeperRFClientLibrary) are **camel Case**.

    &{metrics} =  get Cache Metrics

#### Arguments

Arguments have **2 spaces** between the keyword and each other.

    Verify metric value  ${METRIC_EVICTION}  ${NUM_EXPECTED_EVICTIONS}
  
If the keyword needs more than 3 arguments, place the arguments on separate lines.

```
${request} =  Create test client read request
...  ${fileName}
...  ${startBlock}
...  ${endBlock}
...  ${fileLength}
...  ${lastModified}
...  ${clusterType}
```

Use named arguments for keywords where possible to enhance clarity.

```
Verify cache directory size
        .....
...  expectedCacheSize=${CACHE_MAX_SIZE}
```

For sets of keywords with similar arguments, alignment of arguments is preferred. 

```
[Template]   Test cache eviction
Download requests               runConcurrently=${false}
Concurrently download requests  runConcurrently=${true}
Read requests                   runConcurrently=${false}
Concurrently read requests      runConcurrently=${true}
```


## Custom Keywords

If a test requires more functionality than what Robot Framework can offer
(such as when executing requests using the BookKeeper server), keywords can be created
as functions in `BookKeeperClientRFLibrary`. All public methods in this class
are exposed as keywords to be used by Robot Framework.

In the following example, `getCacheMetrics()` in `BookKeeperClientRFLibrary`
is accessible for use by our custom Robot Framework keyword `Verify metric value`:

```
public Map<String, Double> getCacheMetrics() throws IOException, TException
{
  try (RetryingBookkeeperClient client = createBookKeeperClient()) {
    return client.getCacheMetrics();
  }
}
```

```
Verify metric value
    [Arguments]  ${metricName}  ${expectedValue}
    &{metrics} =  get Cache Metrics
    ...
```
