---
title: "General Concepts"
description: ""
date: 2020-11-12T13:26:54+01:00
lastmod: 2020-11-12T13:26:54+01:00
draft: false
images: []
menu:
    docs:
      parent: "nebulastream"
weight: 30
toc: true
---


In this section, we introduce general concepts that we use in NebulaStream. 


## Data Sources
Data sources are central for data management processing systems like NebulaStream. 
In NebulaStream, we distinguish between logical and physical sources.

### Logical Source

A collection of physical data sources sharing common properties are represented by logical source in NebulaStream.
For example, in a smart city, the collection of streetlamps capable of producing usage data are represented by a logical source.
As there could be thousands of physical streetlamps within a city, NebulaStream uses the concept of logical sources to group a collection of data sources together.
All data sources within the collection share common physical characteristics and the same schema.
As a result, all physical sources mapped to a logical source produce data in the same schema. 
Otherwise, a query will fail during its execution.

### Physical Source

A physical source represents the data produced by a sensor, e.g., an individual streetlamp, or data that is sent to NebulaStream from an external systems, for instance via MQTT.
In the previous example, the logical source streetlamps can contain a physical source streetlamp-1 at some physical location within the city and streetlamp-2 at another. Thus, a logical source combine multiple physical sources.

Currently, NebulaStream handle ingestion from files or via a TCP connection. 
The format of the data must be 'CSV'. 
However, as NebulaStream is designed with extensibility in mind, user can add their own formats.


## Topology
The topology represents the interconnection between workers and the coordinator within a NebulaStream cluster as well as their compute capacities.
As NebulaStream's vision is to support cloud-edge infrastructures, the framework allows running workers on geo-distributed locations.
If NebulaStream is used with NebuLi, the topology is hard-coded as part of the configuration files. 
On the other hand, if NebulaStream is used in distributed mode, the coordinator maintains the topology.

## Catalogs

The NebulaStream coordinator stores the current state of the system in two different catalogs.

### Query Catalog

The query catalog stores information regarding all queries managed by the coordinator.
As a query goes through different phases (`registered`, `scheduling`, `running`, `stopped`, or `failed`), the catalog stores the current status of the query.


### Source Catalog

NebulaStream manages sources in the 'SourceCatalog'. 
Users can add logical sources to the source catalog via queries for the distributed mode and as part of the configurations for the NebuLi mode.
The physical source definition is part of the submitted query. 
Users can attach physical sources, i.e., concrete sources like a fully configured TCP source, to logical sources via queries.
When a user specifies a logical source name in a query, the query ingests data from all attached physical sources.