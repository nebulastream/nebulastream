 # System-Level Test

System-level tests treat NebulaStream as a black box, focusing on testing the system as a whole by executing queries
and verifying the outputs. These tests are defined using declarative test definition files (`.test`, `.test_nightly`),
which contain a series of queries sent to NebulaStream. Although system-level tests may take longer to run compared
to unit tests, they offer several advantages:

- **Independence from Internal Components:** System-level tests do not rely on the internal components of NebulaStream, making it easier to modify these components in the future. When internal changes are made, all system-level tests can typically still be used without modification, whereas unit tests often need to be rewritten.
- **Integration Testing:** They evaluate how all components of the system work together, ensuring the overall functionality and integration of the worker.
- **Efficiency:** A single system-level test can cover scenarios that would otherwise require multiple unit tests, potentially making some unit tests redundant.
- **Simplicity:** These tests are generally easier to write, often requiring only a query to be specified.
- **User Perspective:** System-level tests force us to use our software the same way an end-user uses it, lowering the discrepancy between the development of components internally and the actual user perspective. Additionally, if a user discovers a query that is problematic, we can very easily create a test for it.

The initial design can be found at [DD System-Level Tests](../docs/design/20240724_system_level_tests.md).


## Overview
We structure our tests in the following directories:

| Directory                     | Description                                                             |
|-------------------------------|-------------------------------------------------------------------------|
| [bug](bug)                    | Tests to verify bug fixes                                               |
| [datatype](datatype)          | Tests on individual datatypes                                           |
| [milestone](milestone)        | Set of queries we would like to support in the near future              |
| [operator](operator)          | Tests on individual operators                                           |

We classify test targets using two types of groupings:

- **High-level groupings:** These are determined by the file extension:
    - `.test`: Fast tests intended to be run with every commit.
    - `.test_nightly`: Slow tests intended to be run during nightly checks.
- **Subgroupings:** These are optional and defined within the test files themselves. 
    They are indicated the in-file-comment `# groups: [legacy, join]` for subgroups legacy and join.
    A test can belong to multiple subgroups.

### System-level tests during continuous integration
> Will be implemented with TODO #250 [(link)](https://github.com/nebulastream/nebulastream-public/issues/250)

We perform system-level tests in isolated Docker containers. The testing process begins by launching a Docker container 
running the client. Then, multiple Docker containers are started, each running a worker assigned to different test 
categories: vanilla, performance profiler, code coverage, and thread sanitizer. Depending on the chosen test group and
subgroups, the client distributes queries to all relevant workers.

``` mermaid
---
title: System-Level Test CI Setup
---
flowchart LR
    subgraph "Docker Container 0"
    A(cmake target) -->|Invokes| B(llvm-lit)
    B -->|Discovers| Z[Tests Group Nightly]
    B -->|Discovers| U[Tests Group Vanilla]
    B --> |Test Files| STL(SLT Parser)
    STL --> C(Client)
    end
    subgraph "Docker Container 1"
    C -->|localhost:8080| D(Worker - Vanilla)
    end
    subgraph "Docker Container 2"
    C -->|localhost:8081| E(Worker - Performance Profiler)
    end
    subgraph "Docker Container 3"
    C -->|localhost:8082| F(Worker - Code Coverage)
    end
    subgraph "Docker Container 4"
    C -->|localhost:8083| G(Worker - Thread Sanitizer)
    end
```
