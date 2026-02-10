# The Problem
Our current effort is to increase our testing efforts for NebulaStream. 
System-level tests check a system's correctness by treating it as a 'black box' and test from the outside/end-user perspective. 
An example for NebulaStream would be issuing queries through the query API and testing its correctness.
As pointed out in the discussion [system level tests](https://github.com/nebulastream/nebulastream-public/discussions/20), many open-source data management systems heavily rely on this type of testing. 
Currently, our system-level tests for NebulaStream are limited due to the following reasons:

**P1:** System-level test definitions are more complex than they need to be.
They require boilerplate code to set up and start individual components, even for tests that just check a single query's correctness.
This makes it challenging to write them, maintain them, and get an overview of what is tested.

**P2:** System-level tests are slow. 
Usually, they are required to start the coordinator and the worker, which are issued separately on a test suite-by-test suite basis.
This means that the system is started and shutdown for each test suite and queries are run sequentially.

**P3:** System-level tests are not flexible and often not easy to run. 
We don't provide utilities for system level test such as grouping of tests, running tests in parallel, or running tests with different configurations.

# Goals and Non-Goals
In summary, we want a system test suite with the following properties:

**G1:** Lean declarative test definition. 
It should be possible to write tests in a declarative form.
Test definitions should be self-containing, containing the queries that are issued to NebulaStream, the provided data, and the expected result. (**P1**)

**G2:** Catalog of supported queries and their variations. We want to have a catalog of queries that are tested.
It should be easily visible which queries are tested with which data for demos, papers and external collaborators. (**P1**)

**G3:** Fast system-level tests.
The development cycle should be fast, and system tests should only require a reasonable amount of time to perform.
Queries in test suites should be bundled and run in parallel. (**P2**)

**G4:** Ease of running tests.
In the best case, our framework is one binary/tool with a straightforward interface with configuration options for ease of use. (**P3**)

**G5:** Integrate with the current testing environment.
Tests should be integrated via ctest and integrated into our test targets like our current tests. 
Additionally, the test suite should enable us to run with sanitizers, code coverage tools, and performance profilers. (**P3**)

**G6:** Grouping of tests. 
It should be possible to group tests and run specific groups of tests, e.g., groups for nightly tests, performance regression tests, and tests that require specific external plugins. 
Test can be in multiple groups at the same time. (**P3**)

**NG1:** This design document does not cover tests for the distributed NebulaStream execution.

# Our Proposed Solution

## Declarative test files

### File structure
We use self-descriptive test files inspired by [sqllogictests](https://sqlite.org/sqllogictest/doc/trunk/about.wiki).
The idea is to provide a test as a combination of queries, data, and expected results in a human-readable, comprehensive file (fulfils **G1:**).
The following example shows a test file for the filter operator.

File: `test/operator/filter/Filter.test`
``` SQL
# name: operator/filter/Filter.test
# description: Simple filter tests
# groups: [Filter, Operator, Fast]

# LLVM LIT configuration
# RUN: sed -e "s@SINK@%sink@g" -e "s@TESTDATA@%testdata@g"< %s > %t &&  rm %tsink || true
# RUN: %client test -f %t -s %worker_address && %result_checker %t %tsink

Source window UINT64 id UINT64 value UINT64 timestamp
1,1,1000
12,1,1001
4,1,1002
1,2,2000
11,2,2001
16,2,2002
1,3,3000
11,3,3001
1,3,3003
1,3,3200
1,4,4000
1,5,5000
1,6,6000
1,7,7000
1,8,8000
1,9,9000
1,10,10000
1,11,11000
1,12,12000
1,13,13000
1,14,14000
1,15,15000
1,16,16000
1,17,17000
1,18,18000
1,19,19000
1,20,20000
1,21,21000

# Filter equal
Query::from("window")
    .filter(Attribute("value") == 1)
    .SINK;
----
1,1,1000
12,1,1001
4,1,1002

# Filter less than or equal
Query::from("window")
    .filter(Attribute("id") >= 10)
    .SINK;
----
12,1,1001
11,2,2001
16,2,2002
11,3,3001

# Filter less than or equal
Query::from("window")
    .filter(Attribute("timestamp") <= 10000)
    .SINK;
----
1,1,1000
12,1,1001
4,1,1002
1,2,2000
11,2,2001
16,2,2002
1,3,3000
11,3,3001
1,3,3003
1,3,3200
1,4,4000
1,5,5000
1,6,6000
1,7,7000
1,8,8000
1,9,9000
1,10,10000
```
All test files end with `.test`, and optional postfixes can be added to indicate specific groups, such as `_nightly` 
for slow nightly tests.

The file begins with comments (prefixed by #) that provide metadata about the test, such as its name and a description. 
While these fields are optional, they can give additional context to the test.
Following the metadata, the test is defined through a list of group names, sources, and queries. 
The group name list is optional and serves to categorize tests. 

In this example, the test defines a source named window with a schema that includes `UINT64 id`, `UINT64 value`, and `UINT64 timestamp`. 
Data for the test is provided directly within the file, but data can also be loaded from an external CSV file if needed.

The RUN commands are used by llvm-lit to execute the test during continuous integration. 
These commands involve a series of steps, including preparing the test data and running the test client and result checker.

The test file includes several queries, each followed by an `----` identifier and the expected result. 
Similar to sqllogictests, the result can also be represented as a hash (e.g., `6 values hashing to 777dc`) if including 
the exact result doesn't enhance the file's readability. 
Additionally, tests can be set up to check for specific exceptions (e.g., `Error code 1234`).

## System-level testing suite

### llvm-lit (Continuous Integration)
Our test suite will make use of existing testing tools.
We will use [llvm-lit](https://llvm.org/docs/CommandGuide/lit.html), a testing tool ("an infrastructure for discovering and running arbitrary tests") used in the [llvm-project](https://llvm.org/docs/TestSuiteGuide.html).
llvm-lit can discover tests in a project and provides ease-of-use utilities, e.g., to run them in groups.
llvm-lit requires that each test file contains at the beginning a RUN command that is executed:
```
# Replace SINK and TESTDATA with the actual values and remove the sink file
# RUN: sed -e "s@SINK@%sink@g" -e "s@TESTDATA@%testdata@g"< %s > %t &&  rm %tsink || true

# Run the test with the client and check the result
# RUN: %client test -f %t -s %worker_address && %result_checker %t %tsink
```
The run command can include arbitrary terminal prompts and [substitutions](https://llvm.org/docs/CommandGuide/lit.html#substitutions) such as *%s* for the path to the current test file.
Based on the command's error code llvm-lit decides if the [test status](https://llvm.org/docs/CommandGuide/lit.html#test-status-results), e.g., pass.

In the project root we provide the configuration file `.cfg` that configures the test runner and runs setup tasks such as starting the worker.
Based on the given command line inputs, different versions of the NebulaStream worker will be started (e.g., with or without plugins, compiled with thread sanitizer, etc.).
For each group of tests, we can define a configuration file that specifies the RUN command and the tools to use (`nightly.cfg`, `performanceRegression.cfg`, etc.).

Running all tests is a simple `python3 lit .` call (fulfils **G4**). A specific test group can be run with `llvm-lit --config-prefix=nightly .`.
llvm-lit can be started with `--shuffle` to run tests in random order, and `--filter` to run only tests that match a pattern (fulfils **G6**).
With `-j <threadNum>` we can run test files in parallel (fulfils **G3**)

We can use the tools developed for llvm-lit such as [compare.py](https://llvm.org/docs/TestSuiteGuide.html#displaying-and-analyzing-results) for detailed reports and [LNT](https://llvm.org/docs/TestSuiteGuide.html#continuous-tracking-with-lnt) to continuously track runtime results with a web interface (e.g., for regression tests).
Natively llvm-lit provides a test report as a summary of the test results:

```
PASS: MapExpressionTest (1 of 4)
PASS: FilterOperatorTest (2 of 4)
FAIL: JoinOperatorTest (3 of 4)
******************** TEST 'JoinOperatorTest' FAILED ********************
Test 'JoinOperatorTest' failed as a result of exit code 1234.
********************
PASS: WindowAggregationTest (4 of 4)`
```

### Google Tests (Local Development)

Beneath llvm-lit tests, we provide the option to run test files as Google tests.
They are meant to run locally during development and allow running tests in a debugger.
We provide the targets `ST_<test_name>` for single tests and `STGRP_<group name>` for test groups.

### Test directory structure
All our test files are located in a directory called `tests` at the project root (fulfils **G2**).
Test files in these directories can be assigned to groups by the file ending `.test_regression` or `.test_nighly`.

For new we propose the following directory structure:

| Directory                     | Description                                                             |
|-------------------------------|-------------------------------------------------------------------------|
| [bug](bug)                    | Tests to verify bug fixes                                               |
| [datatype](datatype)          | Tests on individual datatypes                                           |
| [milestone](milestone)        | Set of queries we would like to support in the near future              |
| [operator](operator)          | Tests on individual operators                                           |

# Proof of Concept

We provide a proof of concept at [#234](https://github.com/nebulastream/nebulastream-public/pull/234) and the integration at [#260](https://github.com/nebulastream/nebulastream-public/pull/260).

# Alternatives
We provide llvm-lit for continuous integration and google tests for local development.
Both tools complement each other and fulfill different requirements.

For testing during continuous integration llvm-lit is superior as it provides additional 
[tooling](https://llvm.org/docs/TestSuiteGuide.html) for test discovery and reporting (e.g., `compare.py`, `LNT`).

For local development, tests based on gtest are superior as they integrate with IDEs and debuggers (`gdb`).
Also, gtest allows providing targets similar to existing targets used in the current development process.
Most of this is C++ specific not easily possible with `llvm-lit` which is are more general, language-agnostic tool.

# Further Readings

https://llvm.org/docs/TestSuiteGuide.html

http://lnt.llvm.org/

https://llvm.org/docs/lnt/
