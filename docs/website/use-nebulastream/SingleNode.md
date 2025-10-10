---
title: "NebuLi: Single-Node Deployment"
description: "Understanding NebulaStream's single-node deployment capabilities."
lead: ""
date: 2020-10-13T15:21:01+02:00
lastmod: 2020-10-13T15:21:01+02:00
draft: false
menu:
  docs:
    parent: "use-nebulastream"
weight: 130
toc: true
---

In this section, we describe how NebulaStream operates in a single-node environment, focusing on the `nebuli` client and its interaction with a single NebulaStream worker.

## `query.yaml` Example
NebulaStream queries, along with their associated sources and sinks, can be defined in a `query.yaml` file. Here is an example:

```yaml
query: |
  SELECT * FROM pm_abp_source INTO pm_abp_sink
sink:
  name: pm_abp_sink
  type: MQTT
  config:
    inputFormat: JSON
    serverURI: mqtt://localhost:1883
    clientId: pm-abp-s2s
    topic: sink/live/patient_monitor_abp
    qos: 1
logical:
  - name: pm_abp_source
    schema:
      - name: timestamp
        type: UINT64
      - name: value
        type: FLOAT64
physical:
  - logical: pm_abp_source
    parserConfig:
      type: CSV
      tupleDelimiter: "|"
      fieldDelimiter: ","
    sourceConfig:
      type: MQTT
      serverURI: mqtt://localhost:1883
      topic: source/live/patient_monitor_abp
      qos: 1
```

## Logical Sources
A logical source describes a schema for incoming data. Multiple physical sources can attach to a single logical source. Queries only reference these logical sources, which may then expand to ingest data from all attached physical sources.

## `nebuli` Client for Single-Node Deployment
`nebuli` acts as a client to communicate with a NebulaStream worker. While the worker runs continuously, `nebuli` is ephemeral; it is primarily used to launch, stop, and monitor queries.

`nebuli` is stateless and performs parsing and logical optimization of queries before sending them to the worker.

To connect to a running NebulaStream worker, `nebuli` requires the `-s` parameter to specify the gRPC URI of the worker (e.g., `127.0.0.1:8080`).

Here is the help message for the `nebuli` command-line tool in single-node mode:

```
Usage: nebuli [-d | --debug] <command> [<args>]

NebulaStream Command Line Interface

Options:
  -d, --debug      Dump the Query plan and enable debug logging

Commands:
  register         Register a query
    Usage: nebuli register [-s <server>] [-m <target-machine>] [-i <input>] [-x] <query>
      -s, --server   grpc uri e.g., 127.0.0.1:8080
      -m, --target-machine The hosts machine architecture. This is required when deploying a model. It defaults to host. (Choices: host, generic_x86, rpi5)
      -i, --input    Read the query description. Use - for stdin which is the default (default: "-")
      -x             Immediately start the query after registration (equivalent to calling start) (flag)

  start            Start a query
    Usage: nebuli start <queryId> [-s <server>]
      <queryId>      ID of the query to start
      -s, --server   grpc uri e.g., 127.0.0.1:8080

  stop             Stop a query
    Usage: nebuli stop <queryId> [-s <server>]
      <queryId>      ID of the query to stop
      -s, --server   grpc uri e.0.0.1:8080

  unregister       Unregister a query
    Usage: nebuli unregister <queryId> [-s <server>]
      <queryId>      ID of the query to unregister
      -s, --server   grpc uri e.g., 127.0.0.1:8080

  dump             Dump the DecomposedQueryPlan
    Usage: nebuli dump [-o <output>] [-m <target-machine>] [-i <input>] <query>
      -o, --output   Write the DecomposedQueryPlan to file. Use - for stdout (default: "-")
      -m, --target-machine The hosts machine architecture. This is required when deploying a model. It defaults to host. (Choices: host, generic_x86, rpi5)
      -i, --input    Read the query description. Use - for stdin which is the default (default: "-")
      <query>        Query Statement (default: "")

  status           Observe the state of individual queries
    Usage: nebuli status <queryId> [-s <server>]
      <queryId>      ID of the query to observe
      -s, --server   grpc uri e.g., 127.0.0.1:8080
```

### Query Lifecycle: Register, Start, Stop, and Unregister
There is a clear distinction between `register` and `start` commands:
*   **`register`**: This command loads the query onto the server and performs validation without actually running it. This is useful for testing query validity or preparing multiple queries for later execution.
*   **`start`**: This command initiates the execution of a previously registered query.

The `-x` flag with the `register` command allows you to immediately start the query after registration, combining both steps into one.

Similarly, for stopping queries:
*   **`stop`**: This command pauses a running query. A stopped query can be restarted later.
*   **`unregister`**: This command completely removes a query from the server.
