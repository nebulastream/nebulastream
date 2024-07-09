# The Problem
A core idea of NebulaStream is that it supports millions of sources.
Therefore, supporting a large number of heterogeneous data sources and sinks and making it easy to add new ones is vital to the usability of NebulaStream. A good example for a system that supports a large number of data sources and makes adding new ones easy is [Telegraf](https://github.com/influxdata/telegraf).
Our current implementation is far away from being on the level of Telegraf. The implementation of sources and sinks happened over many years without clear direction. It was executed by various people, including many Hiwis that did not have any prior C++ experience, and it was never refactored, even though we have been talking about a refactor for several years.

The specific problems are the following:
- P1: the implementation of data sources are scattered across an enormous amount of classes
  - LogicalSource and PhysicalSource
  - SourceLogicalOperator (that shares a directory with LogicalSourceDescriptor)
  - source/sink implementations (inherit from DataSource/SinkMedium)
  - descriptors for every single source and sink (inherit from SourceDescriptor/SinkDescriptor) (think 200+ sources/sinks)
  - types for every single source (inherit from PhysicalSourceType) (think 200+ sources)
  - a file with ~2000 lines that handles serialization of operators, including sources and sinks
  - proto files for sources and sinks
  - many places where sources and sinks are lowered (LowerToExecutableQueryPlan, ConvertLogicalToPhysicalSource, ConvertLogicalToPhysicalSink, DefaultPhysicalOperatorProvider, DefaultPhysicalOperatorProvider)
  - factories (DefaultDataSourceProvider, DataSinkProvider, PhysicalOperatorProvider, LogicalSourceTypeFactory, LogicalOperatorFactory, PhysicalSourceTypeFactory)
  - additional implementations for plugins (DataSourcePlugin, DataSinkPlugin, PhysicalSourceFactoryPlugin, SourceDescriptorPlugin, ..)
- P2: all source implementations inherit from DataSource
  - DataSource implements the query start logic, which should be handled by a separate class
  - DataSource provides the allocateBuffer() function which returns a TestTupleBuffer
    - as a result, all sources use the TestTupleBuffer
- P3: adding a single source and sink requires changing/touching 20+ files
- P4: the process of setting up a source and a sink and querying requires defining logical sources in the coordinator config, physical sources in the worker config, using the correct logical source name in a query and defining a physical sink descriptor in the same query
- P5: the process of going from a logical source name in a query to an implementation for that source is unnecessarily complex
- P6: the process of specifying a sink is different from the process of specifying a source and it mixes DQL and DDL syntax in our queries
- P7: the process of going from a physical source definition in a query to an implementation for that sink is unnecessarily complex


# Goals
- G1: Simplify the source implementations. Following the encapsulation principle of OOP, the task of a source should simply be to ingest data and to write that data to a TupleBuffer.
  - addresses P1 and P3
- G2: Starting a query should be handled by a separate class that simply uses a Source implementation.
  - addresses P2
- G3: Handle formatting/parsing separate from ingesting data. A data source simply ingests data. A formatter/parser formats/parses the raw data according to a specific format.
  - addresses P1
- G4: Create a plugin registry that enables us to easily create a new source as an internal or external plugin and that returns a constructor given a source name (or enum if feasible).
  - mainly addresses P3
- G5: unify the process of creating data sources and sinks
  - addresses P4 and P6
  - keep the process of creating sinks as similar as possible to that of creating sources
- G6: Simplify the process of going from a logical source name or a sink name to the implementation of the source(s) or sink(s) as much as possible, especially on the worker side
  - addresses P1,P3, P5, and P7
  - builds on top of prior goals and addresses minimizing the code required to implement source/sink support end to end

# Non-Goals
- refactoring the coordinator side of source/sink handling
  - we allow ourselves to make reasonable assumptions, which must be part of this design document, of how the coordinator will handle sources and sinks in the future
    - this affects P4 in particular

# Proposed Solution
## Fully Specified Source/Sink Descriptor
Currently, we are creating physical sources from source types. Source types are configured using either a `YAML::Node` as input or a `std::map<std::string, std::string>`. Therefore, all current configurations are possible to represent as a map from string to string. We also observe that most configurations use scalar values from the following set {uint32_t, uint64_t, bool, float, char}. Therefore, we conclude that we can model all current source configurations as a `std::unordered_map<std::string, std::variant<uint32_t, uint64_t, std::string, bool, float, char>>`.
Given that everything that a worker needs to know to create a source/sink is the type, and potentially the configuration and meta information, we define the following:

Fully specified source/sink descriptor:
  - the distinct type of the source/sink (one to one mapping from type to source/sink implementation)
  - (optional) the configuration of the source/sink, represented as a `std::unordered_map<std::string, std::variant<uint32_t, uint64_t, std::string, bool, float, char>>`
  - (optional) meta data, such as whether a source allows source sharing, represented as class member variables of the SourceDescriptor and the SinkDescriptor respectively.

Thus, there is only one source descriptor implementation and one sink descriptor implementation. The distinct type identifies which type of source the descriptor describes. The configuration is general enough to handle all source/sink configurations and the meta information are part of the source/sink descriptor. This descriptor contains all information to construct a fully specified source/sink, allowing for 'RESTful queries', i.e., the worker does not require to maintain state concerning sources and sinks to execute queries.

*Open question: does it make sense to only have one descriptor, both for sources and sinks? Intuitively, it seems better to separate both, so that the object type is descriptive (SourceDescriptor vs SinkDescriptor). Additionally, some meta information such as 'source sharing' are exclusive to sources or sinks (in this case sources).*

*Open question: should we simply use 'int' in the variant, instead of uint32_t and uint64_t.*

*Open question: we could also support enums and objects with the variant. However, we will parse the enums/classes from a string anyway, so it might make more sense to resolve the string in respective source/sink.*

## Assumptions
We make the following assumptions:
- Assumption 1: The coordinator allows to create fully specified physical *source* descriptors during runtime
  - when creating a physical source/sink, the user must supply a worker id (or similar)
  - the user must connect the physical source to at least one logical source
  - this assumption is reasonable, since users currently already need to configure physical sources for each worker. During startup the workers then register the physical sources at the coordinator. 
- Assumption 2: The coordinator allows to create fully specified physical *sink* descriptors during runtime
  - physical sinks are currently already specified in a queries at runtime
  - this assumption is reasonable, since the only difference is that the physical sink descriptor is not specified in the analytical (DQL) query, but in a definition (DDL) query beforehand (just like a physical source is created (G5))
- Assumption 3: The coordinator can map all fully specified physical source and sink descriptors to workers
  - this assumption follows naturally from Assumption 1
  - additionally, this assumption is reasonable since it is already given for sources
    - from LogicalSourceExpansionRule:
      ```c++
      //Add to the source operator the id of the physical node where we have to pin the operator
      //NOTE: This is required at the time of placement to know where the source operator is pinned
      duplicateSourceOperator->addProperty(PINNED_WORKER_ID, sourceCatalogEntry->getTopologyNodeId());
      ```
- Assumption 4: The decomposed query plans that a worker receives from the coordinator contains a fully specified source/sink descriptor
  - even though the mapping from logical source names to source types and descriptors currently happens on the worker, this assumption is still reasonable, because:
    - the coordinator and worker can share a module that defines source and sink descriptors
    - the coordinator is already able to map logical source names to physical source names. Based on Assumption 1 and Assumption 2, the coordinator will therefore be able to map logical source names to fully specified source descriptors (sink descriptors are already given)
    - the coordinator is already able to parse query plans with fully specified sink descriptors
- Assumption 5: The coordinator only places decomposed query plans on worker nodes that support the fully specified source/sink descriptors contained in the decomposed query plan
  - this assumption logically follows from Assumption 2 and 3

## Solution
Todo: mermaid diagram that shows on overview of the implementation

We propose the following solution.
## G1
Source implementations now only implement the logic to read data from a certain source, e.g. File, Kafka, or ZMQ, and write the raw bytes received from that source into a TupleBuffer (not a TestTupleBuffer!). This design follows the encapsulation principle of OOP. Additionally, since we write only the raw bytes into a buffer, there is no CSVSource anymore, or a JSONFileSource, or a ParquetSource. There is only a FileSource that reads data from a file. CSV, JSON and Parquet are formats, therefore they are handled by a formatter/parser.

We plan to reduce the supported sources to only the File, ZMQ and the TCP source. We will add the other sources back as soon as the design is fleshed out.
## G2
Currently, the `WorkerRPCServer` calls `NodeEngine::startQuery()`,  which calls `QueryManagerLifecycle::startQuery()` , which calls `DataSource()::start()`. `start()` then spawns a thread and calls `DataSource()::runningRoutine()`, which then calls one of two running routines. Either `runningRoutineWithGatheringInterval()`, or `runningRoutineWithIngestionRate()`.
The running routine is controlled by the `GatheringMode` enum, which is always set to `INTERVAL_MODE` currently. We therefore propose to remove the `GATHERING_INTERVAL`. We simply gather data as fast as possible.

We propose that DataSource becomes a purely virtual interface with two functions `virtual start(EmitFunction &&) = 0` and `virtual stop() = 0`. The DataSource interface is implemented by two different kinds of DataSources.
1. PullingDataSource, which regularly pulls data from a source. Currently, sources like the TCP, Kafka or MQTT source act as pulling sources. They regularly call `receiveData()` to pull data from the respective data source.
2. PushingDataSource, here, a source continuously pushes data into the DataSource. An example would be the `ZMQSource()`. Currently, we spawn a ZMQ server with multiple threads that continuously ingests data and pushes it into the `ZMQSource()`. Therefore we do not interrupt the running routine for the ZMQSource:
    ```c++
    /// If the source is a ZMQSource, the thread must not sleep
    if (getType() != SourceType::ZMQ_SOURCE && gatheringInterval.count() > 0) {
        std::this_thread::sleep_for(gatheringInterval);
    }
    ```
Both types of data sources start a thread and regularly check if they have been requested to stop.

Todo: provide implementation details on starting stopping (will follow with prototype)


## G3
The source itself does not handle the formatting/parsing of the ingested data anymore. The first, observation is that formatting/parsing is tied to formats that may be shared by different sources. For example it is possible to read a JSON file, or to receive JSON via MQTT or TCP or Kafka. The second observation is that formatting is performance-critical. Our current CSV formatting/parsing is so slow that it is a major bottleneck in many experiments. 
By separating formatting/parsing from the source entirely, it becomes possible to make formatting/parsing part of the compiled query. This allows for interesting optimizations such as generating formatting/parsing code for a specific schema and a specific query. For example, a projection could be handled during formatting/parsing already.

For the initial implementation of sources and sinks we use simple runtime implementations (non-compiled) for sources and sinks. We will support CSV and JSON. However, since formatting/parsing is executed separately from the data ingestion it becomes easily possible to create formatting/parsing operators that are compiled to query code in the future.


Todo: provide implementation details on interaction between source and formatter/parser and how the formatter/parser is represented in the query and when it is constructed (will follow with prototype)

## G4
In PR #48 we introduced the design of a static plugin registry. This registry allows to auto-register source and sinks constructors using a string key. In [Assumptions](#assumptions) we described that the worker receives fully specified query plans, were all sink and source names were already resolved to [fully specified source/sink descriptors](#fully-specified-sourcesink-descriptor). Thus, on the worker, after compiling the query, we can construct a source/sink in the following way using a fully specified source/sink descriptor and the source plugin registry:
1. get the distinct type as a string from the descriptor and use it as a key for the plugin registry to construct a `std::unique_ptr` of the specified source/sink type
2. if provided, get the configuration represented as a `std::unordered_map<std::string, std::variant<uint32_t, uint64_t, std::string, bool, float, char>>` from the descriptor and pass it to the newly constructed source/sink to configure it
3. if provided, configure meta data using the meta data configured in the descriptor

We could also pass the descriptor to the constructed source/sink by reference and the source/sink then configures itself and sets the meta data, but that is an implementation detail.

## G5 - Unify Sources and Sinks
### Coordinator exclusive
In [Assumptions](#assumptions) we describe that the coordinator will allow users to create a source or a sink using the same mechanism, which we will specify in an upcoming design document. This addresses the main issue, that physical sources are typically created in YAML files that configure workers and (physical) sinks are created in analytical queries. Furthermore, both source and sink names will be resolved to [fully specified source/sink descriptors](#fully-specified-sourcesink-descriptor) in the same way on the coordinator.
### Worker
The source and sink descriptor both inherit from the same abstract descriptor class. Sources and sinks use different plugin registries, since we might implement some protocols just as a source or sink (an example would be the serial interface to the Philips monitor in the NEEDMI project, where we don't need a sink). However, both registries are purposefully designed in a similar way. By separating the query start/stop logic and the formatting/parsing logic from the source implementation, sources and sinks can be constructed similarly. We construct both sources and sinks in the same pass from source/sink descriptors using the source/sink registries on the worker.


## G6 - Simplify Source/Sink Processing Logic (Worker)
We mainly address this problem by removing the necessity of workers to store any state concerning sources and sinks (see [Assumptions](#assumptions)). Workers receive decomposed queries with [fully specified source/sink descriptors](#fully-specified-sourcesink-descriptor) from the coordinator. In combination with the plugin registry (see solution [G4](#g4)), we allow source/sink descriptors to be dragged through the lowering process until we construct the actual sources and sinks using the registry, requiring only the descriptor.

# Alternatives
In this section, we regard alternatives concerning where to configure physical sources and sinks [A1](#a1---where-to-configure-physical-sources-and-sinks) and how to construct the [A2](#a2---constructing-sources-and-sinks).
## A1 - Where to Configure Physical Sources (and Sinks)
In A1, we regard different alternatives concerning where to configure and where to maintain the state of physical sources and sinks.
### A1.1 - Configure Physical Sources on the worker
This is our current approach. We configure physical sources using YAML files. Configuring a physical source requires a logical- and a physical source name. The logical source name must already exist on the coordinator (must be configured).
#### Advantages
- physical sources are already configured on start up, allowing to submit queries directly after start up
- workers can be opaque to end users that submit analytical queries since everything was configured beforehand
- allows to statically register queries at start up (not yet supported)
  - great for devices that can only be configured during start up
#### Disadvantages
- it is not possible to add new sources after start up
  - if a worker moves into another subnet, configured physical sources may not be supported anymore
- setting up NebulaStream becomes difficult (see [The Problem](#the-problem))
- configuring sinks is different from configuring sources
  - we could also configure sinks in the YAML configuration but that would make the setup even less flexible
- the worker needs to keep track of all registered physical source in addition to the coordinator
- the coordinator and worker need to sync their physical sources (during start up)
#### vs Proposed Solution
- the [Proposed Solution](#proposed-solution) requires configuring sources after startup and looses the potential to deploy queries statically
- long-term the [Proposed Solution](#proposed-solution) enables to mix DDL and DQL queries, meaning that we can create larger queries that register physical sources and sinks and then query the newly created physical sources (and sinks)
  - the associated [discussion](https://github.com/nebulastream/nebulastream-public/discussions/10) describes this vision in more detail
- the [Proposed Solution](#proposed-solution) enables to worker to be stateless concerning sources and sinks
  - similar to REST, our queries contain the state needed to execute the query ([fully specified source/sink descriptors](#fully-specified-sourcesink-descriptor))

### A1.2 - Binding Physical Sources to Workers on the Coordinator
Same as A1.1, but register physical sources via queries on the coordinator. Could potentially combine A1.1 and A1.2, but that would mean managing more registering logic.
#### Advantages
- makes adding sources and sinks after set up possible
- makes a zero configuration NebulaStream a possibility
#### Disadvantages
- requires knowledge of workers in queries addressed at the coordinator (bind calls)
- no more support for statically registered queries (which we don't support yet)
- coordinator and worker need to sync their physical sources
- extra state kept on worker
#### vs Proposed Solution
- the [Proposed Solution](#proposed-solution) goes one step further, by not registering the physical sources and sinks on the worker, enabling the worker to be stateless concerning sources and sinks
  - similar to REST, our queries contain the state needed to execute the query ([fully specified source/sink descriptors](#fully-specified-sourcesink-descriptor))

### A1.3 - Broadcasting
Configure a physical source and attach it to a logical source on the worker using DDL queries. Use the logical source name in a query. The coordinator then sends a broadcast message to all workers inquiring whether they support the specified physical source. The workers respond. All workers that support the physical source become part of the query.
#### Advantages
- does not require knowledge of workers when writing DQL queries
- can help using workers that are otherwise neglected
#### Disadvantages
- potentially spams the system with broadcast messages
- requires a lot of non-existing logic
- difficult to manage authorization (potentially not all nodes that support a source should participate)
- requires synchronization between all workers and the coordinator, which is difficult to implement robustly
#### vs Proposed Solution
- the [Proposed Solution](#proposed-solution) requires manually setting up physical sources and sinks on the coordinator, which requires knowledge about workers and in particular worker ids
- the [Proposed Solution](#proposed-solution) mostly builds on top of existing logic and therefore requires little changes in our system and it is easier to reason about

## A2 - Constructing Sources and Sinks
For the most part, sources and sinks exist as descriptors (see [fully specified source/sink descriptor](#fully-specified-sourcesink-descriptor)). However, to physically consume data, NebulaStream needs to construct the source/sink implementations from the descriptors. We discuss our solution approach concerning how to best construct the sources and sinks implementations in detail #48. The following is a brief summary.

### Factories (Current State)
- factories are well understood and allow us to construct any source/sink
- creating sources/sinks should be limited to very few places in the code, thus, only a few places need to import the factory and we don't need to forward factory objects between classes
- simple factories require big if-else cases on the different source/sink types
  - a source/sink descriptor is given, but it is still necessary to determine the correct constructor
- the factory is part of the core code paths, thus constructing any kind of source/sink is part of the core code paths, thus optional sources, e.g., an OPC source become part of the core code paths
  - this results in #ifdefs along core code paths
  - this results in verbose core code paths (pollution)

#### vs Proposed Solution (Static Plugin Registry)
- the implementation logic static plugin registry (#48) is far more complex than a simple factory
- the static plugin registry enables us to construct sources/sinks using a name from the source/sink descriptor and thereby avoids large if-else branches
- the static plugin registry enables us to create external plugins that are still in-tree, but that are not part of core code paths and that can be activated/deactivated
  - no more/very few #ifdefs
  - less verbose code paths (logic is in (external) plugins)

# Open Questions
Currently, all open questions are contained in the individual sections.

# (Optional) Sources and Further Reading
- [Influx Telegraf webpage](https://www.influxdata.com/time-series-platform/telegraf/)
- [Influx Telegraf Github](https://github.com/influxdata/telegraf)

# (Optional) Appendix