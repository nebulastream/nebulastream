# How to add a `Sink`

Sinks in NebulaStream serve the purpose of exporting intermediate and final query results.
They are the counterpart to sources and represent an interface for data to leave the system.
Due to their similarity with sources, most of this guide is analogous to sources.
(Nearly) everything that can be source can also be a sink (files, network protocols, serial port, etc.).
The most notable difference lies in its interface, which we'll see in chapter [implementation](#4-implementation).

Most of the time, a sink will wrap a client library to interact with the data source we want to connect to and read data from.
For this guide, we'll use the `MQTTSink` as a running example.
The source allows writing data to an MQTT broker via the MQTT network protocol.

## 1. Overview

In `nes-plugins`, we find the implementations of existing plugins.
We create a directory `MQTTSink` for our implementation.
Generally, you can structure this however you see fit and describe the resulting structure in the `CMakeLists.txt`.
In our example, we will use one header and .cpp file for the MQTT sink.

```
nes-plugins/
├── Sources/
├── Sinks/
│   ├── MQTTSink/
│   │   ├── MQTTSink.hpp
│   │   ├── MQTTSink.cpp
│   │   ├── CMakeLists.txt
│   │   └── ...
│   ├── KafkaSink/
│   └── ...
├── Functions/
└── ...
```

## 2. Dependencies & CMake

The cmake configuration mirrors the one described in our source guide.

## 3. Creation & Validation

## 4. Implementation


## 5. Testing
Currently, there is no way to test sinks easily, especially ones that require external systems to be booted up.
However, you can write unit tests if you require any additional logic that does not revolve around I/O.
In the near future, we plan to integrate a toolkit for containerized testing of sources/sinks that interact with external systems.

