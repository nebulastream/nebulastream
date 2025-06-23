# How to add a `Sink`

Sinks in NebulaStream serve the purpose of exporting intermediate and final query results.
They are the counterpart to sources and represent an interface for data to leave the system.
Due to their similarity with sources, most of this guide is analogous to sources.
(Nearly) everything that can be source can also be a sink (files, network protocols, serial port, etc.).
The most notable difference lies in its interface, which we'll see in chapter [implementation](#4-implementation).

Most of the time, a sink will wrap a client library to interact with the data sink we want to connect to and write data to.
For this guide, we'll use the `MQTTSink` as a running example.
The sink allows writing data to an MQTT broker via the MQTT network protocol.

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

Creation and validation of the sinks mirrors sources, with one notable exception, which is formatting.
Within sources, we do not deal with the conversion of the input format into our native tuple format.
Instead, we are allowed to treat the input as a stream of bytes that we simply copy into the input buffer.
As of now, sinks should contain the output formatter that does the reverse conversion as a member.
If we forget this output formatting, we would write the native CPU representation of our tuples into the external system. 
The constructor of the sink including the formatter looks like this:
```c++
MQTTSink::MQTTSink(const SinkDescriptor& sinkDescriptor)
    : Sink()
    , serverUri(sinkDescriptor.getFromConfig(ConfigParametersMQTT::SERVER_URI))
    , clientId(sinkDescriptor.getFromConfig(ConfigParametersMQTT::CLIENT_ID))
    , topic(sinkDescriptor.getFromConfig(ConfigParametersMQTT::TOPIC))
    , qos(sinkDescriptor.getFromConfig(ConfigParametersMQTT::QOS))
{
    switch (const auto inputFormat = sinkDescriptor.getFromConfig(ConfigParametersMQTT::INPUT_FORMAT))
    {
        case Configurations::InputFormat::CSV:
            formatter = std::make_unique<CSVFormat>(sinkDescriptor.schema);
            break;
        case Configurations::InputFormat::JSON:
            formatter = std::make_unique<JSONFormat>(sinkDescriptor.schema);
            break;
        default:
            throw UnknownSinkFormat(fmt::format("Sink format: {} not supported.", magic_enum::enum_name(inputFormat)));
    }
}
```
Generally, it follows the source approach but has a special configuration parameter that we can parse to create the correct output formatter.

## 4. Implementation
Sinks are implemented as their own pipelines that are invoked with a reference to a `TupleBuffer` and are expected to write this buffer into the target system/device.
Their interface is as follows:
```c++
/// Equivalent to `open` in sources
void start(PipelineExecutionContext& pipelineExecutionContext) override;

/// Equivalent to `fillTupleBuffer` in sources
void execute(const Memory::TupleBuffer& inputTupleBuffer, PipelineExecutionContext& pipelineExecutionContext) override;

/// Equivalent to `close` in sources
void stop(PipelineExecutionContext& pipelineExecutionContext) override;
```

It is the analogous interface that sources use.
- `start` will be called once before the first invocation of `execute` and can be used to setup any state or resources necessary.
- `execute` is called repeatedly, however not again with the same data. You need to make sure all data is safe in the target system before returning from this method.
- `stop` is the counterpart of `start`, will be called once before query termination and can be used to clean up and free any I/O resources.

Within the `start` method, we setup our library and initiate a connection to the MQTT broker, such that it is prepared to receive data from us in any subsequent `execute` call.
Any I/O exceptions are caught and rethrown as our internal exception type (to be found in `ExceptionDefinitions.inc`).
We can define it like this:
```c++
void MQTTSink::start(Runtime::Execution::PipelineExecutionContext&)
{
    /// (1) initialize client library
    client = std::make_unique<mqtt::async_client>(serverUri, clientId);

    try
    {
    
        /// (2) setup client connection
        const auto connectOptions = mqtt::connect_options_builder().automatic_reconnect(true).clean_session(true).finalize();
        client->connect(connectOptions)->wait();
    }
    catch (const mqtt::exception& exception)
    {
        /// (3) convert mqtt exception to internal exception
        throw CannotOpenSink(e.what());
    }
}
```

An important difference between sources and sinks is that at the moment, encoding/formatting of buffers is done within the sink itself.
For this reason, we ask the `formatter` to convert our result buffer from native in-memory representation into a string of CSV-encoded tuples that we can write to MQTT.
The `PipelineExecutionContext` variable can be omitted for sinks. 
Operators can use them to emit result buffers into successor pipelines, which is not necessary in the case of sinks.
Our MQTT sink's `execute` method could be implemented like this:
```c++

void MQTTSink::execute(const Memory::TupleBuffer& inputBuffer, Runtime::Execution::PipelineExecutionContext&)
{
    /// (1) Early exit when empty
    if (inputBuffer.getNumberOfTuples() == 0)
    {
        return;
    }

    /// (2) Ask output formatter for formatted buffer, set quality of service for message
    const mqtt::message_ptr message = mqtt::make_message(topic, formatter->getFormattedBuffer(inputBuffer));
    message->set_qos(qos);

    try
    {
        /// (3) Do blocking publish
        client->publish(message)->wait();
    }
    catch (...)
    {
        throw wrapExternalException();
    }
}
```

Finally, `stop` mirrors `start`, ending the connection with the MQTT broker.
Any I/O-related exception during connection termination is caught and only reported, which is acceptable because no further methods on the sink will be called anymore.
```c++
void MQTTSink::stop(Runtime::Execution::PipelineExecutionContext&)
{
    try
    {
        client->disconnect()->wait();
    }
    catch (const mqtt::exception& exception)
    {
        NES_ERROR("Error during stop of sink: {}", exception.what());
    }
}
```

## 5. Testing

Currently, there is no way to test sinks easily, especially ones that require external systems to be booted up.
However, you can write unit tests if you require any additional logic that does not revolve around I/O.
In the near future, we plan to integrate a toolkit for containerized testing of sources/sinks that interact with external systems.
