# Robot Framework Integration Tests

## Test Suite

Any variables needed for test execution should be collected under the *** Variables *** header,
especially if they can be reused by multiple tests in the same suite.

Test cases should be defined as a test template under the *** Test Cases *** header, 
where the template is the keyword containing the actual test keywords to be run.

We are using test templates to allow for repeated execution of the same test 
with different execution methods (explained in the Execution section).

## Test Case

### Setup

Use the **Cache test setup** keyword to start a BookKeeper server
with the provided configuration options.

    Cache test setup
    ...  ${DATADIR}
    ...  rubix.cluster.on-master=true
    ...  hadoop.cache.data.dirprefix.list=${CACHE_DIR_PFX}
    ...  hadoop.cache.data.dirsuffix=${CACHE_DIR_SFX}
    ...  hadoop.cache.data.max.disks=${CACHE_NUM_DISKS}
    ...  rubix.cache.fullness.size=${CACHE_MAX_SIZE}

### Test Body

Integration tests should:
 * generate any files needed for test execution
 * execute whatever steps necessary to sufficiently test the desired scenario
 * verify the state of the BookKeeper & cache using metrics and other helper keywords

#### Generation

Data files can be generated individually:

    ${fileName} =  Generate single test file  ${filePath}  ${fileLength}

or as a batch of files with similar characteristics:

    @{fileNames} =  Generate test files  ${filePathPrefix}  ${fileLength}  ${numberOfFiles}

#### Execution

For read calls sent to the BookKeeper server, current execution modes include combinations of:

* Caching data directly using the Thrift API OR through a CachingFileSystem
* Sequential OR concurrent execution of caching calls

The execution mode is determined by the keyword name passed into the test template.

#### Verification 

Verify the results of the execution steps by comparing metrics values to expected values:

    Verify metric value  ${metricName}  ${expectedValue}
    
as well as checking that the size of the cache is as expected:

    Verify cache directory size
              ...
    ...  ${expectedCacheSize}
    

### Teardown

Make sure to include **Cache test teardown** as a \[Teardown\] step; this ensures
the BookKeeper server used for this test is properly shut down and the environment is cleaned
before execution of the next test.