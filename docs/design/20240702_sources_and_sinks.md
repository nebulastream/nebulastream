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
- G3: Handle formatting separate from ingesting data. A data source simply ingests data. A formatter formats the raw data according to a specific format.
  - addresses P1
- G4: Create a plugin registry that enables us to easily create a new source as an internal or external plugin and that returns a constructor given a source name (or enum if feasible).
  - mainly addresses P3
- G5: unify the process of creating data sources and sinks
  - addresses P4 and P6
  - keep the process of creating sinks as similar as possible to that of creating sources
- G6: Simplify the process of going from a logical source name or a sink name to the implementation of the source(s) or sink(s) as much as possible, especially on the worker side
  - addresses P1,P3, P5, and P7
  - builds on top of prior goals and addresses involves reducing our source/sink implementation to only the code that we think is required

# Non-Goals
- refactoring the coordinator side of source/sink handling
  - we allow ourselves to make reasonable assumptions, which must be part of this design document, of how the coordinator will handle sources and sinks in the future
    - this affects P4 in particular

# Our Proposed Solution
## Assumptions
We define a fully specified source/sink descriptor as a descriptor that contains:
  - the distinct type of the source/sink (one to one mapping from type to source/sink implementation)
  - (optional) the configuration of the source/sink (default if not specified)
  - (optional) meta data, such as whether a source allows source sharing (default if not specified)
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
We propose the following solution.
Todo:
- G1: specify the new source/sink interface
- G2: specify the new data source interface
- G3: specify the raw data formatter interface
- G4: specify how the source/sink plugin registry makes it easy to create new sources and sinks
- G6: specify all classes that are involved in the source/sink process on the worker

## Goals Check
- G1: Simplify the source implementations. Following the encapsulation principle of OOP, the task of a source should simply be to ingest data and to write that data to a TupleBuffer.
  - addresses P1 and P3
- G2: Starting a query should be handled by a separate class that simply uses a Source implementation.
  - addresses P2
- G3: Handle formatting separate from ingesting data. A data source simply ingests data. A formatter formats the raw data according to a specific format.
  - addresses P1
- G4: Create a plugin registry that enables us to easily create a new source as an internal or external plugin and that returns a constructor given a source name (or enum if feasible).
  - mainly addresses P3
- G5: unify the process of creating data sources and sinks
  - addresses P4 and P6
  - keep the process of creating sinks as similar as possible to that of creating sources
- G6: Simplify the process of going from a logical source name or a sink name to the implementation of the source(s) or sink(s) as much as possible, especially on the worker side
  - addresses P1,P3, P5, and P7
  - builds on top of prior goals and addresses involves reducing our source/sink implementation to only the code that we think is required

# Alternatives


# Open Questions

# (Optional) Sources and Further Reading

# (Optional) Appendixcod