---
title: "Overview"
description: ""
lead: ""
date: 2020-11-12T15:22:20+01:00
lastmod: 2020-11-12T15:22:20+01:00
draft: false
images: []
menu:
    docs:
        parent: "nebulastream"
weight: 10
toc: true
---

NebulaStream is the first data management system that is designed for the cloud-edge continuum. 
As a result, NebulaStream is capable of incorporating all computing resources of an infrastructure, even nodes outside the cloud, and applying processing wherever possible. 
The only requirement for participating is that the node is capable of running a NebulaStream Worker and has at least one connection to an already existing member of the NebulaStream infrastructure.

In general, we provide three components in NebulaStream:
- **NebulaStream Worker:** The worker is the execution engine to execute queries and create results. Every worker provides a GRPC interface for the communication with other components. Note that, this communication has to adhere to our internal format, for instance the query plan must be in NebulaStream's internal format. 
- **NebuLi:** NebuLi is our state-less, light-weight optimizer component that is capable of taking an SQL-like query string, applying a basic set of optimization, e.g., type inference or source expansion, and deploying the query plans to a set of workers. Note that, as NebuLi is state-less, it does not persist information about the topology or running queries.
We provide a CLI for NebuLi and also a library that can be integrated by other components or external applications to interact with the NebulaStream Worker. For more information, see [NebulaStream Single Deployment]({{< ref "SingleNode" >}}).
- **Coordinator:** The coordinator is a state-full component that manages the entire NebulaStream Cluster. Note that, in the current release, we provide this coordinator only in an experimental state and will stabilize it as part of upcoming releases. For more information, see [NebulaStream Distributed Deployment]({{< ref "Distributed" >}}).

We refer to our [Vision Systems Paper](https://www.nebula.stream/paper/zeuch_cidr20.pdf)
and [Application Paper](https://www.nebula.stream/paper/zeuch_vliot.pdf) for more details on the vision and use-cases of NebulaStream.

# Available Features

## Worker
In the current release, NebulaStream consists of a stable NebulaStream Worker that is well tested and allows for robust query execution. 
Its design centers around an extensive, easy-to-use, and efficient single node engine.
The worker supports multi-modal, multi-frequency workloads for data sources that are attached to it. 
In particular, in the current release, we support a basic set of features for the single node execution:
- Executing queries, see [Get Started]({{< ref "GetStarted" >}})
- Applying a basic set of operations for data manipulation, see our [Query API]({{< ref "APIConcepts" >}})
- Compiling queries to efficient code
- Execution on ARM, Intel, and AMD CPUs
- Basic monitoring capabilities

## NebuLi
In the current release, NebuLi provides the following functionality:
- Start, Stop, Register, Unregister a query from a set of NebulaStream workers
- Applying a basic set of rewriting and light-weight optimization rules
- Translate a query in SQL-Like string into a query plan in our NebulaStream's internal format

## Coordinator
The current version of the Coordinator provides a REST interface to interact with the users and persists information about queries and the topology. Internally, it uses the NebuLi client to interact with workers. 
Please note, as the NebulaStream Coordinator is experimental, we do not guarantee a stable, distributed execution for now. 
However, this functionality will be added in the next release as it is on the core of NebulaStream's vision.

## Addition of new features
We structure the development in NebulaStream into a stable core that is maintained by the NebulaStream Maintainer Team and experimental features that are provided by researchers and other collaborators. Initially, all new features start in forks or branches and will be merged into the core if they are 1) of relevance for many users, 2) of high code quality, 3) fulfill our guidelines, and 4) if they are stable enough to be shipped as a core feature. If a feature passes this requirements, it will be integrated into the core of NebulaStream and the NebulaStream Maintainer Team will guarantee its robust processing.

As one of NebulaStreams design goals is *extensibility*, it provides a convenient plug-in mechanism to extend the stable core of NebulaStream without the requirement of changing the core itself. 

# Roadmap
As NebulaStream is a system that is driven by researchers, we divide the roadmap into research features that would be available as extensions and core features that extend the stable core.

## Core Features
- **Distributed Query Processing:** We plan to support distributed query execution as a first class citizen in the next release.
- **Null Handling:** We will extend NebulaStream to handle Null values as this is required by many use cases.


## Research Features
- **Fault-Tolerance:** We plan to integrate our recent work on fault tolerance. 
- **Spatial Processing:** We plan to extend NebulaStream through spatial operations. 
- **Adaptive Sensor Management:** We plan to integrate our latest work on adaptive sensor management.
- **Complex Event Processing:** We plan to integrate a basic set of CEP operators.