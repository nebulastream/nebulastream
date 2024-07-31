# System Level Test

We provide tests that test NebulaStream on the system level.
These tests treat NebulaStream as blackbox and aim to test it holistically.
We use declarative test definition files (`.test`, `.test_nightly`) which consist of a sequence of queries to NebulaStream.
The initial design can be found at [DD System Level Tests]().


## Overview
We structure our tests in the following directories:

| Directory    | Description                                                                       |
|--------------|-----------------------------------------------------------------------------------|
| benchmark    | Tests on the correct execution of known benchmark suites (e.g., YSB, Nexmark, ...) |
| bug          | Tests to verify bug fixes                                                         |
| datatype     | Tests on datatypes (e.g., Int, Text, ...)                                         |
| errorhandling | Tests on correct error handling i.e. the correct error codes are thrown           |
| milestone    | Set of queries we would like to support in the near future                        |
| optimizer    | Tests on optimizer optimizations                                                  |
| parser       | Tests on correct parsing                                                          |
| plugin       | Tests on plugin features                                                          |

Addtionally, there are two types of groupings for test targets: i) high-level groups determined by the file ending 
(e.g., `.test_nightly` for slow test meant to be run at nightly checks, or `.test` for fast tests meant to be run at 
every commit) and ii) an optional subgrouping within the high-level group defined in the files (e.g.,`# groups: [legacy]`).
Tests are allowed to be in multiple subgroups.

## Run System Level Tests Locally

Upon building the project, we create targets for each of the test files for two types of tests: i) local test targets and ii) CI test targets:

|                   | Local               | CI                   |
|-------------------|---------------------|----------------------|
| framework         | gtest               | llvm-lit + gtest     |
| target            | ST_<test file name> | LIT_<test file name> |
| debuggable        | yes                 | no                   |
| parallelism       | intra file          | inter file           |
| caches input plan | yes                 | no                   |


## System Level Tests during CI
> CI integration files are not part of this PoC

We run system level test on separate docker containers. 
Upon testing, we start one docker container running the client.
We start multiple docker containers running the workers with different tests categories: vanilla, performance profiler, code coverage, thread sanitizer.
Based on the selected test group and subgroups, the client sends the queries to all the corresponding workers.

``` mermaid
---
title: System Level Test CI Setup
---
flowchart LR
    subgraph Docker Container 0
    A(cmake target) -->|Invokes| B(llvm-lit)
    B -->|Discovers| Z[Tests Group Nightly]
    B -->|Discovers| U[Tests Group Vanilla]
    B --> |Test Files| STL(SLT Parser)
    STL --> C(Client)
    end
    subgraph Docker Container 1
    C -->|localhost:8080| D(Worker - Vanilla)
    end
    subgraph Docker Container 2
    C -->|localhost:8081| E(Worker - Performance Profiler)
    end
    subgraph Docker Container 3
    C -->|localhost:8082| F(Worker - Code Coverage)
    end
    subgraph "Docker Container 4"
    C -->|localhost:8083| G(Worker - Thread Sanatizer)
    end
```
