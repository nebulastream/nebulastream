# The Problem
A core idea of NebulaStream is that it supports millions of sources.
Therefore, supporting a large number of heterogeneous data sources and sinks and making it easy to add new ones is vital to the usability of NebulaStream. A good example for a system that supports a large number of data sources and makes adding new ones easy is [Telegraf](https://github.com/influxdata/telegraf).
Our current implementation is far away from being on the level of Telegraf. The implementation of sources and sinks happened over many years without clear direction. It was executed by various people, including many Hiwis that did not have any prior C++ experience, and it was never refactored, even though we have been talking about a refactor for several years.

The specific problems are the following:
- the implementation of data sources are scattered across an enormous amount of classes
  - LogicalSource and PhysicalSource
  - SourceLogicalOperator that shares a directory with LogicalSourceDescriptor
  - Descriptors for every single 
- all source implementations inherit from DataSource
  - DataSource implements the query start logic, which should be handled by a separate class
  - DataSource provides the allocateBuffer() function which returns a TestTupleBuffer
    - as a result, all sources use the TestTupleBuffer
- adding a single source and sink requires changing/touching ~20 files
- the process of setting up a source and a sink and querying it is unnecessarily complex
- the process of going from a logical source name in a query to an implementation for that source is unnecessarily complex
- the process of specifying a sink mixes DQL and DDL syntax in our queries
- the process of going from a physical source definition in a query to an implementation for that sink is unnecessarily complex


## Context
- sources and sinks grew over time and were never refactored
- many sources and sinks were implemented by HIWIs

# Goals and Non-Goals
- G1: Simplify the source implementations. Following the encapsulation principle, the task of a source should simply be to ingest data and to write that data to a TupleBuffer.
- G2: Starting a query should be handled by a separate class that simply uses a Source implementation.
- G3: Handle formatting separate from ingesting data. A data source simply ingests data. A formatter formats the raw data according to a specific format.
- G5: Create a plugin registry that enables us to easily create a new source as an internal or external plugin.
- G4: Reimplement a selection of sources using as internal plugins, avoiding the TestTupleBuffer.
- G5: Simplify the process of going from a logical source name to the implementation of the source(s) as much as possible
- G6: unify the process of creating data sources and sinks
- G7: simplify the process of going from a sink definition in a query to a physical sink
  - keep the process as similar as possible to that of going from a logical source name to the source implementations

# Our Proposed Solution

# Alternatives

# Open Questions

# (Optional) Sources and Further Reading

# (Optional) Appendix