# Changelog

## [v0.2.75](https://github.com/nebulastream/nebulastream/tree/v0.2.75) (2022-08-08)

[Full Changelog](https://github.com/nebulastream/nebulastream/compare/v0.2.74...v0.2.75)

### Query Optimizer 🔧

- Fix ILP placement algorithm to follow new interface for placement strategy [\#2485](https://github.com/nebulastream/nebulastream/issues/2485)

## [v0.2.74](https://github.com/nebulastream/nebulastream/tree/v0.2.74) (2022-08-08)

[Full Changelog](https://github.com/nebulastream/nebulastream/compare/v0.2.73...v0.2.74)

#### User Defined Functions

- \[Feature\] Serialize UdfCallExpressionNodes [\#2883](https://github.com/nebulastream/nebulastream/issues/2883)

## [v0.2.73](https://github.com/nebulastream/nebulastream/tree/v0.2.73) (2022-08-04)

[Full Changelog](https://github.com/nebulastream/nebulastream/compare/v0.2.72...v0.2.73)

### Miscellaneous Issues ✌️

- \[Feature\] Move source and query catalog to catalog namespace [\#2903](https://github.com/nebulastream/nebulastream/issues/2903)

## [v0.2.72](https://github.com/nebulastream/nebulastream/tree/v0.2.72) (2022-08-03)

[Full Changelog](https://github.com/nebulastream/nebulastream/compare/v0.2.71...v0.2.72)

#### Monitoring 🚥

- \[Feature\] Create NEMO Placement Strategy [\#2859](https://github.com/nebulastream/nebulastream/issues/2859)

## [v0.2.71](https://github.com/nebulastream/nebulastream/tree/v0.2.71) (2022-08-01)

[Full Changelog](https://github.com/nebulastream/nebulastream/compare/v0.2.70...v0.2.71)

#### Spatial Query Processing 🌇

- \[Feature\] Serialize and de-serialize shape expressions [\#2887](https://github.com/nebulastream/nebulastream/issues/2887)

## [v0.2.70](https://github.com/nebulastream/nebulastream/tree/v0.2.70) (2022-07-28)

[Full Changelog](https://github.com/nebulastream/nebulastream/compare/v0.2.69...v0.2.70)

### Miscellaneous Issues ✌️

- Refactor NESRequests so not all require a query ID [\#2896](https://github.com/nebulastream/nebulastream/issues/2896)

## [v0.2.69](https://github.com/nebulastream/nebulastream/tree/v0.2.69) (2022-07-28)

[Full Changelog](https://github.com/nebulastream/nebulastream/compare/v0.2.68...v0.2.69)

## [v0.2.68](https://github.com/nebulastream/nebulastream/tree/v0.2.68) (2022-07-28)

[Full Changelog](https://github.com/nebulastream/nebulastream/compare/v0.2.67...v0.2.68)

#### REST API 

- Move to oatapp REST server: start oatapp server on NES startup [\#2770](https://github.com/nebulastream/nebulastream/issues/2770)

## [v0.2.67](https://github.com/nebulastream/nebulastream/tree/v0.2.67) (2022-07-27)

[Full Changelog](https://github.com/nebulastream/nebulastream/compare/v0.2.66...v0.2.67)

**Implemented enhancements:**

- Refactoring Physical Stream Config and Source Config [\#2311](https://github.com/nebulastream/nebulastream/issues/2311)

### Bug Fixes 🐛

- \[BUG\] Fix data race when Worker is shutdown [\#2912](https://github.com/nebulastream/nebulastream/issues/2912)

#### Fault Tolerance

- Upstream backup with non-blocking storage [\#2880](https://github.com/nebulastream/nebulastream/issues/2880)

## [v0.2.66](https://github.com/nebulastream/nebulastream/tree/v0.2.66) (2022-07-26)

[Full Changelog](https://github.com/nebulastream/nebulastream/compare/v0.2.65...v0.2.66)

### Bug Fixes 🐛

- \[BUG\] Multi-aggregation logic outputs the name of the stream multiple times [\#2885](https://github.com/nebulastream/nebulastream/issues/2885)

## [v0.2.65](https://github.com/nebulastream/nebulastream/tree/v0.2.65) (2022-07-26)

[Full Changelog](https://github.com/nebulastream/nebulastream/compare/v0.2.64...v0.2.65)

#### User Defined Functions

- \[Feature\] add ExpressionNodes for Python UDFs [\#2799](https://github.com/nebulastream/nebulastream/issues/2799)

## [v0.2.64](https://github.com/nebulastream/nebulastream/tree/v0.2.64) (2022-07-22)

[Full Changelog](https://github.com/nebulastream/nebulastream/compare/v0.2.63...v0.2.64)

### Bug Fixes 🐛

- \[BUG\] Fix Hierarchical Topology with ParentID via Config [\#2908](https://github.com/nebulastream/nebulastream/issues/2908)

## [v0.2.63](https://github.com/nebulastream/nebulastream/tree/v0.2.63) (2022-07-19)

[Full Changelog](https://github.com/nebulastream/nebulastream/compare/v0.2.62...v0.2.63)

### Bug Fixes 🐛

- \[BUG\] increase timeout for arm ci [\#2890](https://github.com/nebulastream/nebulastream/issues/2890)

#### Build Management

- \[Feature\] Enable parallel ci build [\#2893](https://github.com/nebulastream/nebulastream/issues/2893)

## [v0.2.62](https://github.com/nebulastream/nebulastream/tree/v0.2.62) (2022-07-18)

[Full Changelog](https://github.com/nebulastream/nebulastream/compare/v0.2.61...v0.2.62)

### Miscellaneous Issues ✌️

- MLIR: Extend Types and Arithmetic Operations and Logical Comparisons [\#2854](https://github.com/nebulastream/nebulastream/issues/2854)

## [v0.2.61](https://github.com/nebulastream/nebulastream/tree/v0.2.61) (2022-07-15)

[Full Changelog](https://github.com/nebulastream/nebulastream/compare/v0.2.60...v0.2.61)

### Bug Fixes 🐛

- \[BUG\] Unable to start NebulaStream on AWS instances [\#2899](https://github.com/nebulastream/nebulastream/issues/2899)

#### Fault Tolerance

- Speed up Upstream Backup [\#2798](https://github.com/nebulastream/nebulastream/issues/2798)

## [v0.2.60](https://github.com/nebulastream/nebulastream/tree/v0.2.60) (2022-07-07)

[Full Changelog](https://github.com/nebulastream/nebulastream/compare/v0.2.59...v0.2.60)

#### Spatial Query Processing 🌇

- \[Feature\] Write unit tests for ShapeExpressions and SpatialPredicates [\#2886](https://github.com/nebulastream/nebulastream/issues/2886)

## [v0.2.59](https://github.com/nebulastream/nebulastream/tree/v0.2.59) (2022-07-06)

[Full Changelog](https://github.com/nebulastream/nebulastream/compare/v0.2.58...v0.2.59)

### Bug Fixes 🐛

- \[BUG\] Add tests for KTM use case [\#2871](https://github.com/nebulastream/nebulastream/issues/2871)

## [v0.2.58](https://github.com/nebulastream/nebulastream/tree/v0.2.58) (2022-07-04)

[Full Changelog](https://github.com/nebulastream/nebulastream/compare/v0.2.57...v0.2.58)

### Bug Fixes 🐛

- \[BUG\] Sending wrong Query Plan operators via rest [\#2850](https://github.com/nebulastream/nebulastream/issues/2850)

## [v0.2.57](https://github.com/nebulastream/nebulastream/tree/v0.2.57) (2022-07-01)

[Full Changelog](https://github.com/nebulastream/nebulastream/compare/v0.2.56...v0.2.57)

## [v0.2.56](https://github.com/nebulastream/nebulastream/tree/v0.2.56) (2022-06-30)

[Full Changelog](https://github.com/nebulastream/nebulastream/compare/v0.2.55...v0.2.56)

#### Spatial Query Processing 🌇

- \[Feature\] Convert ConstantExpressions to ShapeExpressionNodes in GeographyExpressions [\#2851](https://github.com/nebulastream/nebulastream/issues/2851)

## [v0.2.55](https://github.com/nebulastream/nebulastream/tree/v0.2.55) (2022-06-30)

[Full Changelog](https://github.com/nebulastream/nebulastream/compare/v0.2.54...v0.2.55)

### Miscellaneous Issues ✌️

- Handle failing of a distributed query when one operator fails [\#2585](https://github.com/nebulastream/nebulastream/issues/2585)

## [v0.2.54](https://github.com/nebulastream/nebulastream/tree/v0.2.54) (2022-06-24)

[Full Changelog](https://github.com/nebulastream/nebulastream/compare/v0.2.53...v0.2.54)

### Miscellaneous Issues ✌️

- \[IR\] Integrate Symbolic Execution to derive trace [\#2825](https://github.com/nebulastream/nebulastream/issues/2825)

## [v0.2.53](https://github.com/nebulastream/nebulastream/tree/v0.2.53) (2022-06-23)

[Full Changelog](https://github.com/nebulastream/nebulastream/compare/v0.2.52...v0.2.53)

### Bug Fixes 🐛

- \[BUG\] Fix Compiler Cache [\#2826](https://github.com/nebulastream/nebulastream/issues/2826)

## [v0.2.52](https://github.com/nebulastream/nebulastream/tree/v0.2.52) (2022-06-22)

[Full Changelog](https://github.com/nebulastream/nebulastream/compare/v0.2.51...v0.2.52)

### Miscellaneous Issues ✌️

- \[Feature\] Refactor UdfCatalog & add PythonDescriptor [\#2845](https://github.com/nebulastream/nebulastream/issues/2845)

## [v0.2.51](https://github.com/nebulastream/nebulastream/tree/v0.2.51) (2022-06-21)

[Full Changelog](https://github.com/nebulastream/nebulastream/compare/v0.2.50...v0.2.51)

### Bug Fixes 🐛

- Incorrect number of origin ids during unionWith [\#2830](https://github.com/nebulastream/nebulastream/issues/2830)

## [v0.2.50](https://github.com/nebulastream/nebulastream/tree/v0.2.50) (2022-06-19)

[Full Changelog](https://github.com/nebulastream/nebulastream/compare/v0.2.49...v0.2.50)

#### Query Compiler

- NESIR: Add Print Functionality [\#2834](https://github.com/nebulastream/nebulastream/issues/2834)

## [v0.2.49](https://github.com/nebulastream/nebulastream/tree/v0.2.49) (2022-06-18)

[Full Changelog](https://github.com/nebulastream/nebulastream/compare/v0.2.48...v0.2.49)

#### Monitoring 🚥

- Enable user defined monitoring as data streams [\#2620](https://github.com/nebulastream/nebulastream/issues/2620)

## [v0.2.48](https://github.com/nebulastream/nebulastream/tree/v0.2.48) (2022-06-13)

[Full Changelog](https://github.com/nebulastream/nebulastream/compare/v0.2.47...v0.2.48)

## [v0.2.47](https://github.com/nebulastream/nebulastream/tree/v0.2.47) (2022-06-10)

[Full Changelog](https://github.com/nebulastream/nebulastream/compare/v0.2.46...v0.2.47)

### Bug Fixes 🐛

- \[BUG\] remove debug output in lambda source [\#2832](https://github.com/nebulastream/nebulastream/issues/2832)

## [v0.2.46](https://github.com/nebulastream/nebulastream/tree/v0.2.46) (2022-06-08)

[Full Changelog](https://github.com/nebulastream/nebulastream/compare/v0.2.45...v0.2.46)

#### Spatial Query Processing 🌇

- Add Geography Expressions [\#2338](https://github.com/nebulastream/nebulastream/issues/2338)

## [v0.2.45](https://github.com/nebulastream/nebulastream/tree/v0.2.45) (2022-06-07)

[Full Changelog](https://github.com/nebulastream/nebulastream/compare/v0.2.44...v0.2.45)

### Bug Fixes 🐛

- \[BUG\] CSV parser does not handle comma decimal points [\#2828](https://github.com/nebulastream/nebulastream/issues/2828)

## [v0.2.44](https://github.com/nebulastream/nebulastream/tree/v0.2.44) (2022-06-01)

[Full Changelog](https://github.com/nebulastream/nebulastream/compare/v0.2.43...v0.2.44)

### Miscellaneous Issues ✌️

- \[Feature\] Prototype of operator Implementation Framework [\#2819](https://github.com/nebulastream/nebulastream/issues/2819)
- Prepare Batch Join implementation for pull request [\#2692](https://github.com/nebulastream/nebulastream/issues/2692)

## [v0.2.43](https://github.com/nebulastream/nebulastream/tree/v0.2.43) (2022-06-01)

[Full Changelog](https://github.com/nebulastream/nebulastream/compare/v0.2.42...v0.2.43)

#### Runtime 

- \[Feature\] Source Sharing at Node [\#2790](https://github.com/nebulastream/nebulastream/issues/2790)

#### Documentation 

- Create a design document for embedding of CUDA kernels to NES [\#2777](https://github.com/nebulastream/nebulastream/issues/2777)

## [v0.2.42](https://github.com/nebulastream/nebulastream/tree/v0.2.42) (2022-05-31)

[Full Changelog](https://github.com/nebulastream/nebulastream/compare/v0.2.41...v0.2.42)

#### Operators 

- \[Feature\] Enable thread local window aggregation for global windows [\#2800](https://github.com/nebulastream/nebulastream/issues/2800)

### Miscellaneous Issues ✌️

- \[Windowing\] add functions to directly access the IngestionTS of a record during execution [\#2548](https://github.com/nebulastream/nebulastream/issues/2548)

## [v0.2.41](https://github.com/nebulastream/nebulastream/tree/v0.2.41) (2022-05-29)

[Full Changelog](https://github.com/nebulastream/nebulastream/compare/v0.2.40...v0.2.41)

#### Runtime 

- Batch Join Prototype [\#2429](https://github.com/nebulastream/nebulastream/issues/2429)

## [v0.2.40](https://github.com/nebulastream/nebulastream/tree/v0.2.40) (2022-05-29)

[Full Changelog](https://github.com/nebulastream/nebulastream/compare/v0.2.39...v0.2.40)

#### Query Compiler

- Integrate MLIR Query Compiler Approach [\#2748](https://github.com/nebulastream/nebulastream/issues/2748)

## [v0.2.39](https://github.com/nebulastream/nebulastream/tree/v0.2.39) (2022-05-28)

[Full Changelog](https://github.com/nebulastream/nebulastream/compare/v0.2.38...v0.2.39)

#### Build Management

- Generate automated release notes [\#2259](https://github.com/nebulastream/nebulastream/issues/2259)

### Miscellaneous Issues ✌️

- \[Feature\] Make Location Data Accessible via REST interface [\#2782](https://github.com/nebulastream/nebulastream/issues/2782)

## [v0.2.38](https://github.com/nebulastream/nebulastream/tree/v0.2.38) (2022-05-27)

[Full Changelog](https://github.com/nebulastream/nebulastream/compare/v0.2.37...v0.2.38)

### Miscellaneous Issues ✌️

- Make timeout configurable in HealthCheckService [\#2714](https://github.com/nebulastream/nebulastream/issues/2714)

## [v0.2.37](https://github.com/nebulastream/nebulastream/tree/v0.2.37) (2022-05-26)

[Full Changelog](https://github.com/nebulastream/nebulastream/compare/v0.2.36...v0.2.37)

### Query Optimizer 🔧

- Fix Manual placement algorithm to follow new interface for placement strategy [\#2487](https://github.com/nebulastream/nebulastream/issues/2487)

### Miscellaneous Issues ✌️

- Revise Query Lifecycle Monitoring [\#2580](https://github.com/nebulastream/nebulastream/issues/2580)
- Implement NES demo for ELEGANT partners [\#2523](https://github.com/nebulastream/nebulastream/issues/2523)

## [v0.2.36](https://github.com/nebulastream/nebulastream/tree/v0.2.36) (2022-05-17)

[Full Changelog](https://github.com/nebulastream/nebulastream/compare/v0.2.35...v0.2.36)

#### User Defined Functions

- \[Feature\] PhysicalOperator for python UDF [\#2779](https://github.com/nebulastream/nebulastream/issues/2779)

## [v0.2.35](https://github.com/nebulastream/nebulastream/tree/v0.2.35) (2022-05-16)

[Full Changelog](https://github.com/nebulastream/nebulastream/compare/v0.2.34...v0.2.35)

### Miscellaneous Issues ✌️

- \[Feature\] Stabilize thread-local keyed window operator [\#2776](https://github.com/nebulastream/nebulastream/issues/2776)
- Add CI process for pushing tags to Demo repo/image [\#2698](https://github.com/nebulastream/nebulastream/issues/2698)

## [v0.2.34](https://github.com/nebulastream/nebulastream/tree/v0.2.34) (2022-05-12)

[Full Changelog](https://github.com/nebulastream/nebulastream/compare/v0.2.33...v0.2.34)

**Implemented enhancements:**

- Adding protobuf support to post requests in SourceCatalogController [\#2785](https://github.com/nebulastream/nebulastream/issues/2785)

## [v0.2.33](https://github.com/nebulastream/nebulastream/tree/v0.2.33) (2022-05-12)

[Full Changelog](https://github.com/nebulastream/nebulastream/compare/v0.2.32...v0.2.33)

### Miscellaneous Issues ✌️

- \[Feature\] Print Stats [\#2802](https://github.com/nebulastream/nebulastream/issues/2802)
- Handle forceful stopping of a distributed query [\#2584](https://github.com/nebulastream/nebulastream/issues/2584)

## [v0.2.32](https://github.com/nebulastream/nebulastream/tree/v0.2.32) (2022-05-09)

[Full Changelog](https://github.com/nebulastream/nebulastream/compare/v0.2.31...v0.2.32)

#### Fault Tolerance

- Introduce RPC for global failure  [\#2672](https://github.com/nebulastream/nebulastream/issues/2672)

### Miscellaneous Issues ✌️

- Add own jemalloc [\#2407](https://github.com/nebulastream/nebulastream/issues/2407)

## [v0.2.31](https://github.com/nebulastream/nebulastream/tree/v0.2.31) (2022-05-08)

[Full Changelog](https://github.com/nebulastream/nebulastream/compare/v0.2.30...v0.2.31)

### Bug Fixes 🐛

- \[BUG\] QueryManager and Benchmarking [\#2771](https://github.com/nebulastream/nebulastream/issues/2771)

## [v0.2.30](https://github.com/nebulastream/nebulastream/tree/v0.2.30) (2022-05-06)

[Full Changelog](https://github.com/nebulastream/nebulastream/compare/v0.2.29...v0.2.30)

### Bug Fixes 🐛

- \[BUG\] Unnecessary copy of config in worker [\#2791](https://github.com/nebulastream/nebulastream/issues/2791)

## [v0.2.29](https://github.com/nebulastream/nebulastream/tree/v0.2.29) (2022-05-06)

[Full Changelog](https://github.com/nebulastream/nebulastream/compare/v0.2.28...v0.2.29)

#### Monitoring 🚥

- \[Feature\] Extend metric store to support continuous insertion [\#2786](https://github.com/nebulastream/nebulastream/issues/2786)

## [v0.2.28](https://github.com/nebulastream/nebulastream/tree/v0.2.28) (2022-05-05)

[Full Changelog](https://github.com/nebulastream/nebulastream/compare/v0.2.27...v0.2.28)

### Miscellaneous Issues ✌️

- Move all geolocation logic out of Topology and out of Worker into a location service class [\#2497](https://github.com/nebulastream/nebulastream/issues/2497)

## [v0.2.27](https://github.com/nebulastream/nebulastream/tree/v0.2.27) (2022-05-04)

[Full Changelog](https://github.com/nebulastream/nebulastream/compare/v0.2.26...v0.2.27)

### Miscellaneous Issues ✌️

- Extend QueryExecutionTest to invoke python [\#2753](https://github.com/nebulastream/nebulastream/issues/2753)

## [v0.2.26](https://github.com/nebulastream/nebulastream/tree/v0.2.26) (2022-05-03)

[Full Changelog](https://github.com/nebulastream/nebulastream/compare/v0.2.25...v0.2.26)

#### Fault Tolerance

- Create an integration test for Upstream Backup [\#2755](https://github.com/nebulastream/nebulastream/issues/2755)

## [v0.2.25](https://github.com/nebulastream/nebulastream/tree/v0.2.25) (2022-05-02)

[Full Changelog](https://github.com/nebulastream/nebulastream/compare/v0.2.24...v0.2.25)

### Miscellaneous Issues ✌️

- \[Feature\] Update to LLVM 14 [\#2759](https://github.com/nebulastream/nebulastream/issues/2759)

## [v0.2.24](https://github.com/nebulastream/nebulastream/tree/v0.2.24) (2022-05-02)

[Full Changelog](https://github.com/nebulastream/nebulastream/compare/v0.2.23...v0.2.24)

#### Runtime 

- Check LockFreeWatermarkProcessor [\#2540](https://github.com/nebulastream/nebulastream/issues/2540)

## [v0.2.23](https://github.com/nebulastream/nebulastream/tree/v0.2.23) (2022-04-26)

[Full Changelog](https://github.com/nebulastream/nebulastream/compare/v0.2.22...v0.2.23)

### Bug Fixes 🐛

- Fix memory leak in TestHarness::getOutput [\#2510](https://github.com/nebulastream/nebulastream/issues/2510)

## [v0.2.22](https://github.com/nebulastream/nebulastream/tree/v0.2.22) (2022-04-25)

[Full Changelog](https://github.com/nebulastream/nebulastream/compare/v0.2.21...v0.2.22)

### Miscellaneous Issues ✌️

- \[Feature\] Update to ubuntu 22.04 [\#2767](https://github.com/nebulastream/nebulastream/issues/2767)

## [v0.2.21](https://github.com/nebulastream/nebulastream/tree/v0.2.21) (2022-04-25)

[Full Changelog](https://github.com/nebulastream/nebulastream/compare/v0.2.20...v0.2.21)

### Bug Fixes 🐛

- \[BUG\] [\#2772](https://github.com/nebulastream/nebulastream/issues/2772)
- Query with projection and union fails [\#2524](https://github.com/nebulastream/nebulastream/issues/2524)

## [v0.2.20](https://github.com/nebulastream/nebulastream/tree/v0.2.20) (2022-04-22)

[Full Changelog](https://github.com/nebulastream/nebulastream/compare/v0.2.19...v0.2.20)

### Bug Fixes 🐛

- \[BUG\] RemoteClient Error Handling [\#2763](https://github.com/nebulastream/nebulastream/issues/2763)

## [v0.2.19](https://github.com/nebulastream/nebulastream/tree/v0.2.19) (2022-04-21)

[Full Changelog](https://github.com/nebulastream/nebulastream/compare/v0.2.18...v0.2.19)

#### Fault Tolerance

- Add WatermarkProcessor to the Final Sink [\#2543](https://github.com/nebulastream/nebulastream/issues/2543)

## [v0.2.18](https://github.com/nebulastream/nebulastream/tree/v0.2.18) (2022-04-20)

[Full Changelog](https://github.com/nebulastream/nebulastream/compare/v0.2.17...v0.2.18)

### Bug Fixes 🐛

- \[BUG\] source-tests fail on macOS [\#2744](https://github.com/nebulastream/nebulastream/issues/2744)
- Bug: memory\_resource not supported on Ubuntu 18.04 [\#2410](https://github.com/nebulastream/nebulastream/issues/2410)

## [v0.2.17](https://github.com/nebulastream/nebulastream/tree/v0.2.17) (2022-04-19)

[Full Changelog](https://github.com/nebulastream/nebulastream/compare/v0.2.16...v0.2.17)

### Miscellaneous Issues ✌️

- Add prettyPrintTupleBuffer on column layout [\#2504](https://github.com/nebulastream/nebulastream/issues/2504)

## [v0.2.16](https://github.com/nebulastream/nebulastream/tree/v0.2.16) (2022-04-19)

[Full Changelog](https://github.com/nebulastream/nebulastream/compare/v0.2.15...v0.2.16)

### Miscellaneous Issues ✌️

- \[Refactor Query Manager\] [\#2754](https://github.com/nebulastream/nebulastream/issues/2754)

## [v0.2.15](https://github.com/nebulastream/nebulastream/tree/v0.2.15) (2022-04-15)

[Full Changelog](https://github.com/nebulastream/nebulastream/compare/v0.2.14...v0.2.15)

#### Fault Tolerance

- Inject ReconfigurationTask into datastream and make it follow topology [\#2511](https://github.com/nebulastream/nebulastream/issues/2511)

## [v0.2.14](https://github.com/nebulastream/nebulastream/tree/v0.2.14) (2022-04-14)

[Full Changelog](https://github.com/nebulastream/nebulastream/compare/v0.2.13...v0.2.14)

### Miscellaneous Issues ✌️

- Issue templates to report bugs or request features or create arbitrary issue [\#2658](https://github.com/nebulastream/nebulastream/issues/2658)

## [v0.2.13](https://github.com/nebulastream/nebulastream/tree/v0.2.13) (2022-04-13)

[Full Changelog](https://github.com/nebulastream/nebulastream/compare/v0.2.12...v0.2.13)

### Bug Fixes 🐛

- REST client returns no output/error for an aggregation over a non-existing field [\#2724](https://github.com/nebulastream/nebulastream/issues/2724)

#### Monitoring 🚥

- Create Documentation of Monitoring REST [\#2363](https://github.com/nebulastream/nebulastream/issues/2363)

#### REST API 

- Revisit the response codes returned by the Query Controller  [\#2671](https://github.com/nebulastream/nebulastream/issues/2671)

## [v0.2.12](https://github.com/nebulastream/nebulastream/tree/v0.2.12) (2022-04-12)

[Full Changelog](https://github.com/nebulastream/nebulastream/compare/v0.2.11...v0.2.12)

### Miscellaneous Issues ✌️

- Provision to communicate failure reason for a query [\#2611](https://github.com/nebulastream/nebulastream/issues/2611)

## [v0.2.11](https://github.com/nebulastream/nebulastream/tree/v0.2.11) (2022-04-11)

[Full Changelog](https://github.com/nebulastream/nebulastream/compare/v0.2.10...v0.2.11)

## [v0.2.10](https://github.com/nebulastream/nebulastream/tree/v0.2.10) (2022-04-11)

[Full Changelog](https://github.com/nebulastream/nebulastream/compare/v0.2.9...v0.2.10)

### Bug Fixes 🐛

- \[BUG\] Illegal instruction \(Illegal operand \[0x7fa3df649dfe\]\) whens starting coordinator/worker [\#2742](https://github.com/nebulastream/nebulastream/issues/2742)

## [v0.2.9](https://github.com/nebulastream/nebulastream/tree/v0.2.9) (2022-04-10)

[Full Changelog](https://github.com/nebulastream/nebulastream/compare/v0.2.8...v0.2.9)

### Bug Fixes 🐛

- \[BUG\] No MQTT support in release images [\#2749](https://github.com/nebulastream/nebulastream/issues/2749)

## [v0.2.8](https://github.com/nebulastream/nebulastream/tree/v0.2.8) (2022-04-08)

[Full Changelog](https://github.com/nebulastream/nebulastream/compare/v0.2.7...v0.2.8)

## [v0.2.7](https://github.com/nebulastream/nebulastream/tree/v0.2.7) (2022-04-08)

[Full Changelog](https://github.com/nebulastream/nebulastream/compare/v0.2.6...v0.2.7)

### Bug Fixes 🐛

- \[BUG\] CI is not checking status correctly.  [\#2746](https://github.com/nebulastream/nebulastream/issues/2746)

## [v0.2.6](https://github.com/nebulastream/nebulastream/tree/v0.2.6) (2022-04-07)

[Full Changelog](https://github.com/nebulastream/nebulastream/compare/v0.2.5...v0.2.6)

## [v0.2.5](https://github.com/nebulastream/nebulastream/tree/v0.2.5) (2022-04-06)

[Full Changelog](https://github.com/nebulastream/nebulastream/compare/v0.2.4...v0.2.5)

### Bug Fixes 🐛

- \[BUG\] Fix logger for benchmark repo [\#2735](https://github.com/nebulastream/nebulastream/issues/2735)

### Miscellaneous Issues ✌️

- \[Performance Test\] [\#2734](https://github.com/nebulastream/nebulastream/issues/2734)
- Update Github Readme and Issue Templase [\#2689](https://github.com/nebulastream/nebulastream/issues/2689)
- The REST API \(via e.g., curl\) must be able to correctly submit a new query \(with some info, e.g., query id\) or provide a detailed error message [\#2610](https://github.com/nebulastream/nebulastream/issues/2610)
- The REST API \(via e.g., curl\)   must be able to correctly cancel a running query or provide a detailed error message [\#2608](https://github.com/nebulastream/nebulastream/issues/2608)
- The REST API \(via e.g., curl\) must be able to tell the status of a query to the user \(e.g., running, deployed, stopped, failed\) [\#2606](https://github.com/nebulastream/nebulastream/issues/2606)
- The REST API \(via e.g., curl\)  must be able to correctly inform the user about the reason of a query failure [\#2604](https://github.com/nebulastream/nebulastream/issues/2604)
- The REST API \(via e.g., curl\) must be able to correctly provide the query plan of a running query \(or an error message\) [\#2602](https://github.com/nebulastream/nebulastream/issues/2602)
- Releasing first beta version of NebulaStream to general public [\#2490](https://github.com/nebulastream/nebulastream/issues/2490)

## [v0.2.4](https://github.com/nebulastream/nebulastream/tree/v0.2.4) (2022-04-05)

[Full Changelog](https://github.com/nebulastream/nebulastream/compare/v0.2.3...v0.2.4)

### Bug Fixes 🐛

- Remove irrelevant info in C++ source documentation [\#2704](https://github.com/nebulastream/nebulastream/issues/2704)

## [v0.2.3](https://github.com/nebulastream/nebulastream/tree/v0.2.3) (2022-04-04)

[Full Changelog](https://github.com/nebulastream/nebulastream/compare/v0.2.2...v0.2.3)

### Bug Fixes 🐛

- Local build of dependencies does not build correct LLVM version. [\#2532](https://github.com/nebulastream/nebulastream/issues/2532)

## [v0.2.2](https://github.com/nebulastream/nebulastream/tree/v0.2.2) (2022-04-01)

[Full Changelog](https://github.com/nebulastream/nebulastream/compare/v0.2.1...v0.2.2)

### Bug Fixes 🐛

- Bring backward.hpp to a better state [\#2701](https://github.com/nebulastream/nebulastream/issues/2701)
- Fix release workflow to appropriately tag the executable image  [\#2699](https://github.com/nebulastream/nebulastream/issues/2699)

### Miscellaneous Issues ✌️

- Provide source access to ELEGANT partners [\#2519](https://github.com/nebulastream/nebulastream/issues/2519)

## [v0.2.1](https://github.com/nebulastream/nebulastream/tree/v0.2.1) (2022-03-28)

[Full Changelog](https://github.com/nebulastream/nebulastream/compare/v0.2.0...v0.2.1)

### Miscellaneous Issues ✌️

- Add back multi-arch executable images [\#2675](https://github.com/nebulastream/nebulastream/issues/2675)

## [v0.2.0](https://github.com/nebulastream/nebulastream/tree/v0.2.0) (2022-03-24)

[Full Changelog](https://github.com/nebulastream/nebulastream/compare/v0.1.0...v0.2.0)

**Implemented enhancements:**

- Possibility to pass configuration file to the executable docker image [\#2481](https://github.com/nebulastream/nebulastream/issues/2481)
- Add Guard Once to the repository   [\#2326](https://github.com/nebulastream/nebulastream/issues/2326)
- Change source config to accommodate multiple physical sources in one worker [\#2312](https://github.com/nebulastream/nebulastream/issues/2312)

### Bug Fixes 🐛

- Update for Docker the Ubuntu-based Dev Image for CLion  [\#2685](https://github.com/nebulastream/nebulastream/issues/2685)
- Coordinator logical source configuration schema types should match NES strings [\#2656](https://github.com/nebulastream/nebulastream/issues/2656)
- \[Monitoring\] MetricCollectorTest is flanky [\#2649](https://github.com/nebulastream/nebulastream/issues/2649)
- MonitoringIntegrationTest leads to build errors [\#2646](https://github.com/nebulastream/nebulastream/issues/2646)
- Fix source exit and wrong input file path [\#2639](https://github.com/nebulastream/nebulastream/issues/2639)
- CPU detection is not portable and does not work in Docker image on arm Macs [\#2634](https://github.com/nebulastream/nebulastream/issues/2634)
- Fix issue with Window join slice store logic [\#2633](https://github.com/nebulastream/nebulastream/issues/2633)
- DataSource operators pinned to same processor [\#2550](https://github.com/nebulastream/nebulastream/issues/2550)
- Gathering interval aka Frequency configuration has no effect on data source routine  [\#2546](https://github.com/nebulastream/nebulastream/issues/2546)
- Parameters are not recognized when passed in CLI [\#2531](https://github.com/nebulastream/nebulastream/issues/2531)
- \[MacOS\] MonitoringSerializationTest fails [\#2527](https://github.com/nebulastream/nebulastream/issues/2527)
- \[MacOS\] MonitoringIntegrationTest fails [\#2526](https://github.com/nebulastream/nebulastream/issues/2526)
- CSV sink does not contain expected output [\#2520](https://github.com/nebulastream/nebulastream/issues/2520)
- ERROR TwoLambdaSources - Test  [\#2512](https://github.com/nebulastream/nebulastream/issues/2512)
- Fix ZMQ socket limit [\#2509](https://github.com/nebulastream/nebulastream/issues/2509)
- \[BUG\] Handle query submission without logical source [\#2488](https://github.com/nebulastream/nebulastream/issues/2488)
- MonitoringTests fail on different local Ubuntu Versions [\#2478](https://github.com/nebulastream/nebulastream/issues/2478)
- Prevent unaligned atomics for the buffer control block [\#2476](https://github.com/nebulastream/nebulastream/issues/2476)
- Docker tag and github tag are not in sync [\#2471](https://github.com/nebulastream/nebulastream/issues/2471)
- Executable image and binary installations not working due to relocation of includes [\#2468](https://github.com/nebulastream/nebulastream/issues/2468)
- Solve timestamp issue for seqWith operator [\#2454](https://github.com/nebulastream/nebulastream/issues/2454)
- Doxigen is not working [\#2441](https://github.com/nebulastream/nebulastream/issues/2441)
- Randomly failing OrOperatorTest.testPatternOrMap [\#2436](https://github.com/nebulastream/nebulastream/issues/2436)
- No check for existence of logical stream in query submission [\#2435](https://github.com/nebulastream/nebulastream/issues/2435)
- MQTT fails on Macos [\#2431](https://github.com/nebulastream/nebulastream/issues/2431)
- LLD causes error on MacOs [\#2426](https://github.com/nebulastream/nebulastream/issues/2426)
- Missing Source Configuration in nes-executable-image [\#2423](https://github.com/nebulastream/nebulastream/issues/2423)
- Shutdown bug [\#2418](https://github.com/nebulastream/nebulastream/issues/2418)
- Fix trace node creation [\#2408](https://github.com/nebulastream/nebulastream/issues/2408)
- Support for Ubuntu 21.10 [\#2366](https://github.com/nebulastream/nebulastream/issues/2366)
- Use source buffer configuration correctly [\#2365](https://github.com/nebulastream/nebulastream/issues/2365)
- Perform placement of query with more than one source operator with same logical stream name [\#2295](https://github.com/nebulastream/nebulastream/issues/2295)
- Failing MonitoringStackTest.testMetricGroup [\#2260](https://github.com/nebulastream/nebulastream/issues/2260)
- Bump \(and use\) clang 13 [\#2248](https://github.com/nebulastream/nebulastream/issues/2248)

### Query Optimizer 🔧

- Allow For Incremental Placement of Merged Query Plans [\#2334](https://github.com/nebulastream/nebulastream/issues/2334)
- Store intermediate optimized query plans in the query catalog [\#2229](https://github.com/nebulastream/nebulastream/issues/2229)

#### Monitoring 🚥

- Check MonitoringIntegrationTest on ubuntu 21.10 [\#2482](https://github.com/nebulastream/nebulastream/issues/2482)
- Create interface for SystemResourcesReader [\#2364](https://github.com/nebulastream/nebulastream/issues/2364)

#### Complex Event Processing

- missing const declaration for subQuery of orWith and andwith  [\#2397](https://github.com/nebulastream/nebulastream/issues/2397)
- Test Cases for OR operator  [\#2350](https://github.com/nebulastream/nebulastream/issues/2350)
- Create a sufficient Test Set fo the binary CEP operators  [\#2270](https://github.com/nebulastream/nebulastream/issues/2270)

#### Runtime 

- Refactor QueryManager [\#2455](https://github.com/nebulastream/nebulastream/issues/2455)

#### Query Compiler

- Initial Setup for NES on GPU [\#2324](https://github.com/nebulastream/nebulastream/issues/2324)
- Add table source type for static data [\#2316](https://github.com/nebulastream/nebulastream/issues/2316)

#### Fault Tolerance

- Timestamp oriented BufferStorage [\#2508](https://github.com/nebulastream/nebulastream/issues/2508)
- Message exchange between Sink-Coordinator-Source [\#2473](https://github.com/nebulastream/nebulastream/issues/2473)
- Enable NesWorker to react to errors [\#2457](https://github.com/nebulastream/nebulastream/issues/2457)
- Implement acknowledgment exchange in Upstream backup [\#2374](https://github.com/nebulastream/nebulastream/issues/2374)

#### Build Management

- Introduce nes-compiler component [\#2472](https://github.com/nebulastream/nebulastream/issues/2472)
- Introduce nes-common [\#2448](https://github.com/nebulastream/nebulastream/issues/2448)
- Static Lib for MQTT [\#2438](https://github.com/nebulastream/nebulastream/issues/2438)
- Gtest from dependencies [\#2406](https://github.com/nebulastream/nebulastream/issues/2406)

#### Documentation 

- Prepare documentation for release [\#2680](https://github.com/nebulastream/nebulastream/issues/2680)
- Add Documentation to Configure Coordinator, Worker, Sink, Sources [\#2677](https://github.com/nebulastream/nebulastream/issues/2677)
- Add to Documention of Ide: Docker as a toolchain + build env in clion [\#2676](https://github.com/nebulastream/nebulastream/issues/2676)
- Add page describing configuration options [\#2669](https://github.com/nebulastream/nebulastream/issues/2669)
- Improve Code Guidelines [\#2661](https://github.com/nebulastream/nebulastream/issues/2661)
- Add documentation for the "Contribution guildelines" [\#2598](https://github.com/nebulastream/nebulastream/issues/2598)
- Add documentation for the NebulaStream UI [\#2597](https://github.com/nebulastream/nebulastream/issues/2597)
- Add documentation for the REST API [\#2596](https://github.com/nebulastream/nebulastream/issues/2596)
- Add documentation for CEP [\#2595](https://github.com/nebulastream/nebulastream/issues/2595)
- Add Documentation for the "First steps" [\#2594](https://github.com/nebulastream/nebulastream/issues/2594)
- Contribution guildelines [\#2573](https://github.com/nebulastream/nebulastream/issues/2573)
- Setup new Documentation Framework  [\#2570](https://github.com/nebulastream/nebulastream/issues/2570)
- Add documentation for the "Building/Installing" [\#2566](https://github.com/nebulastream/nebulastream/issues/2566)
- Add documentation for the "Components Overiew: Logical physical streams, Worker, Coordinator, Topology" [\#2565](https://github.com/nebulastream/nebulastream/issues/2565)
- Add general documentation for the "Overview of Architecture" [\#2564](https://github.com/nebulastream/nebulastream/issues/2564)
- Add documentation for the Query API [\#2563](https://github.com/nebulastream/nebulastream/issues/2563)
- Add documentation "How to use NES:" Hello World example [\#2562](https://github.com/nebulastream/nebulastream/issues/2562)
- Add documentation "Learn about NES" [\#2561](https://github.com/nebulastream/nebulastream/issues/2561)
- Provide a structured and up to date documentation of NES Java Client [\#2558](https://github.com/nebulastream/nebulastream/issues/2558)
- Create Wordcount-style demo [\#2557](https://github.com/nebulastream/nebulastream/issues/2557)
- Documentation on how to install NebulaStream \(from package manager, or from source\) [\#2554](https://github.com/nebulastream/nebulastream/issues/2554)

### Miscellaneous Issues ✌️

- Fix VERSION\_POSTFIX on tag branch [\#2696](https://github.com/nebulastream/nebulastream/issues/2696)
- Create Debian Package for different architectures and systems [\#2687](https://github.com/nebulastream/nebulastream/issues/2687)
- Allow defining logical source with schema configuration from Coordinator [\#2645](https://github.com/nebulastream/nebulastream/issues/2645)
- Fixes for pr \#2332: Use forward declarations, move unused variables into ifdef, fix mem-leaks and use smart pointers [\#2627](https://github.com/nebulastream/nebulastream/issues/2627)
- \[CPP Client\] add CMAKE configuration to enable inclusion in external projects [\#2618](https://github.com/nebulastream/nebulastream/issues/2618)
- The C++ client must be able to correctly cancel a running query or provide a detailed error message [\#2607](https://github.com/nebulastream/nebulastream/issues/2607)
- The C++ client must be able to tell the status of a query to the user \(e.g., running, deployed, stopped, failed\) [\#2605](https://github.com/nebulastream/nebulastream/issues/2605)
- The C++ client must be able to correctly provide the query plan of a running query \(or an error message\) [\#2601](https://github.com/nebulastream/nebulastream/issues/2601)
- A client must be able to correctly provide the query plan of a running query \(or an error message\) [\#2592](https://github.com/nebulastream/nebulastream/issues/2592)
- A client must be able to tell the status of a query to the user \(e.g., running, deployed, stopped, failed\) [\#2590](https://github.com/nebulastream/nebulastream/issues/2590)
- Query cancellation via any client must result in a query to be cancelled or a detailed error message [\#2589](https://github.com/nebulastream/nebulastream/issues/2589)
- Query submission via any client must result in a query to be executed or a detailed error message [\#2588](https://github.com/nebulastream/nebulastream/issues/2588)
- Clear up logging statements in performance-sensitive locations  [\#2587](https://github.com/nebulastream/nebulastream/issues/2587)
- Create Access path for source [\#2583](https://github.com/nebulastream/nebulastream/issues/2583)
- Create Access path for binary [\#2582](https://github.com/nebulastream/nebulastream/issues/2582)
- Improve documentation on how to contribute to a NebulaStream project. [\#2572](https://github.com/nebulastream/nebulastream/issues/2572)
- Decide how to divide documentation between user documentation \(on the homepage\) and developer/contributor documentation \(possibly in a wiki, e.g., Github wiki\) @he-sk \(Critical\) [\#2571](https://github.com/nebulastream/nebulastream/issues/2571)
- Introduce heart-beat mechanism to detect NesWorker/NesCoordinator failures [\#2569](https://github.com/nebulastream/nebulastream/issues/2569)
- Reduce docker build, development, and executable image size [\#2556](https://github.com/nebulastream/nebulastream/issues/2556)
- Remove unneeded headers from the binary distribution @digiou \(deb, docker, etc\). [\#2555](https://github.com/nebulastream/nebulastream/issues/2555)
- Provision to perform major and minor releases. [\#2552](https://github.com/nebulastream/nebulastream/issues/2552)
- Assign unique origin ids for all source operators. [\#2551](https://github.com/nebulastream/nebulastream/issues/2551)
- Refactor Logger [\#2534](https://github.com/nebulastream/nebulastream/issues/2534)
- Reduce the number of header files included in the binary of NES [\#2530](https://github.com/nebulastream/nebulastream/issues/2530)
- Replace NodeEngineFactory with NodeEngineBuilder [\#2528](https://github.com/nebulastream/nebulastream/issues/2528)
- Provide more stable docker image for use case partners [\#2522](https://github.com/nebulastream/nebulastream/issues/2522)
- MacOS: Ci is blocking because we only have a single action [\#2518](https://github.com/nebulastream/nebulastream/issues/2518)
- CMAKE: Download of dependency package fails [\#2516](https://github.com/nebulastream/nebulastream/issues/2516)
- \[Experimental\] Reuse same source if possible [\#2503](https://github.com/nebulastream/nebulastream/issues/2503)
- Enable Ninja support [\#2499](https://github.com/nebulastream/nebulastream/issues/2499)
- Disable NUMA in the default case.  [\#2492](https://github.com/nebulastream/nebulastream/issues/2492)
- Refactor distribution of binaries [\#2467](https://github.com/nebulastream/nebulastream/issues/2467)
- Reduce boilerplate in ConfigOption [\#2462](https://github.com/nebulastream/nebulastream/issues/2462)
- Update Licence  [\#2459](https://github.com/nebulastream/nebulastream/issues/2459)
- Rename stream to source in our code base [\#2451](https://github.com/nebulastream/nebulastream/issues/2451)
- Allow TupleBuffer to have multiple GC callbacks [\#2449](https://github.com/nebulastream/nebulastream/issues/2449)
- Enhance Wrapper for CUDA kernels [\#2446](https://github.com/nebulastream/nebulastream/issues/2446)
- Add macos to CI [\#2416](https://github.com/nebulastream/nebulastream/issues/2416)
- Allow users to submit maintenance requests requests over REST [\#2403](https://github.com/nebulastream/nebulastream/issues/2403)
- Remove the adaptive build ifdefs [\#2400](https://github.com/nebulastream/nebulastream/issues/2400)
- \[Experimental\] static thread assignment [\#2399](https://github.com/nebulastream/nebulastream/issues/2399)
- Add Function  stubs to WorkerRPCClient/Server to support buffering of data and reconfiguration of NetworkSinks. Add corresponding functions to NodeEngine [\#2394](https://github.com/nebulastream/nebulastream/issues/2394)
- Deployment and storage of tables on worker [\#2382](https://github.com/nebulastream/nebulastream/issues/2382)
- Add maintenance flag to topology node and adjust path finding algorithms to ignore nodes marked for maintenance [\#2352](https://github.com/nebulastream/nebulastream/issues/2352)
- Multi Query Stats [\#2347](https://github.com/nebulastream/nebulastream/issues/2347)
- Add query management functionality to cpp client [\#2346](https://github.com/nebulastream/nebulastream/issues/2346)
- Add source/sink-based materialized view classes [\#2342](https://github.com/nebulastream/nebulastream/issues/2342)
- Add functionalitiy to update geo-spatial information about nodes in topology [\#2332](https://github.com/nebulastream/nebulastream/issues/2332)
- Add option to set location of a worker node [\#2331](https://github.com/nebulastream/nebulastream/issues/2331)
- Introduce RPC endpoint to let a worker notify query failure to coordinator [\#2322](https://github.com/nebulastream/nebulastream/issues/2322)
- Add an S2PointIndex to the Topology class [\#2308](https://github.com/nebulastream/nebulastream/issues/2308)
- Print time unit in Timer [\#2304](https://github.com/nebulastream/nebulastream/issues/2304)
- Key windows by N subsequent attributes [\#2294](https://github.com/nebulastream/nebulastream/issues/2294)
- Key windows by multiple \(combined\) attributes [\#2293](https://github.com/nebulastream/nebulastream/issues/2293)

## [v0.1.0](https://github.com/nebulastream/nebulastream/tree/v0.1.0) (2021-12-15)

[Full Changelog](https://github.com/nebulastream/nebulastream/compare/750122800935da477ec6affaa1e55b7eb9a2fb65...v0.1.0)

### Bug Fixes 🐛

- detail::getExecutablePath\(\) is not sufficient for finding libnes.dylib in runtime linking [\#2330](https://github.com/nebulastream/nebulastream/issues/2330)
- SystemResourcesReader::readStaticNesMetrics is broken on MacOS  [\#2307](https://github.com/nebulastream/nebulastream/issues/2307)
- Reduce usage of `json.h` from cpprestsdk [\#2306](https://github.com/nebulastream/nebulastream/issues/2306)
- Build is broken on ARM [\#2282](https://github.com/nebulastream/nebulastream/issues/2282)
- Build and execute source code and tests that requires NUMA [\#2271](https://github.com/nebulastream/nebulastream/issues/2271)
- Build is failing on macOS because of missing std::string includes [\#2250](https://github.com/nebulastream/nebulastream/issues/2250)
- Collection of Metrics fails on PIs [\#2247](https://github.com/nebulastream/nebulastream/issues/2247)
- Bug in Worker Config [\#2240](https://github.com/nebulastream/nebulastream/issues/2240)
- WorkerRPCServer::GetMonitoringData\(\) fails when assigning dynamic buffer [\#2226](https://github.com/nebulastream/nebulastream/issues/2226)

#### Monitoring 🚥

- Configurable Monitoring [\#2321](https://github.com/nebulastream/nebulastream/issues/2321)

#### Complex Event Processing

- Update NES Wiki after AND is merged to master  [\#2237](https://github.com/nebulastream/nebulastream/issues/2237)
- Remove KeyAssignment from AND Operator  [\#2236](https://github.com/nebulastream/nebulastream/issues/2236)

#### Fault Tolerance

- Query flag to enable fault-tolerance [\#2298](https://github.com/nebulastream/nebulastream/issues/2298)
- Introduce LineageManager [\#2264](https://github.com/nebulastream/nebulastream/issues/2264)

#### Build Management

- Build dependencies locally [\#2313](https://github.com/nebulastream/nebulastream/issues/2313)
- Fail Arm build when code compilation fails  [\#2277](https://github.com/nebulastream/nebulastream/issues/2277)
- Enable build process on arm servers [\#2263](https://github.com/nebulastream/nebulastream/issues/2263)

### Miscellaneous Issues ✌️

- Bump Version to 0.1 [\#2362](https://github.com/nebulastream/nebulastream/issues/2362)
- FixStreamOrigin for OR Operator  [\#2349](https://github.com/nebulastream/nebulastream/issues/2349)
- MQTTSource: Improve Error Handling [\#2348](https://github.com/nebulastream/nebulastream/issues/2348)
- Docker nes-dev-image is missing `perf` for CLion's profiler [\#2318](https://github.com/nebulastream/nebulastream/issues/2318)
- Improve stateless performance [\#2278](https://github.com/nebulastream/nebulastream/issues/2278)
- Fix WindowAggregatrionDescriptor to allow functional Query API [\#2276](https://github.com/nebulastream/nebulastream/issues/2276)
- Move all benchmarking to the benchmark repo [\#2275](https://github.com/nebulastream/nebulastream/issues/2275)
- Replace pragma once with ifndef [\#2261](https://github.com/nebulastream/nebulastream/issues/2261)
- YSB query benchmark in NES [\#2257](https://github.com/nebulastream/nebulastream/issues/2257)
- LRB queries benchmark in NES [\#2256](https://github.com/nebulastream/nebulastream/issues/2256)
- SmartGrid Benchmark in NES [\#2255](https://github.com/nebulastream/nebulastream/issues/2255)
- Introduce Hardware Performance Sampling  [\#2253](https://github.com/nebulastream/nebulastream/issues/2253)
- Introduce Benchmark Source [\#2252](https://github.com/nebulastream/nebulastream/issues/2252)
- Move getNextNodeEngineId and getNextTaskId generators to appropriate classes [\#2244](https://github.com/nebulastream/nebulastream/issues/2244)
- Release Notes Dokument [\#2243](https://github.com/nebulastream/nebulastream/issues/2243)
- Improve error handling when starting the coordinator from the command line [\#2232](https://github.com/nebulastream/nebulastream/issues/2232)



