---
title: "How to add a Source"
weight: 20
---

# How to add a `Source` in NebulaStream

In NebulaStream, sources are responsible for ingesting data into the system.
They typically connect to external systems or hardware through communication channels—such as a client library or kernel API—to read data from a file, network, serial device, or other medium.

Common examples of sources:

- File
- TCP
- MQTT
- Kafka
- PostgreSQL
- Serial Port

These can represent external software systems, networking protocols, or hardware devices.

Most of the time, a source will wrap a client library to interact with the data source we want to connect to and read data from.
This guide walks you through implementing a source plugin using MQTTSource as an example.
MQTT allows reading data from an MQTT broker via the MQTT network protocol and is popular in the IoT space.

## 1. Overview

Plugins are implemented in the `nes-plugins` top-level directory. 
For each new source, create a new directory under `Sources/`, such as `MQTTSource/`. 
This directory will contain your implementation.
Generally, you can structure this however you see fit and describe the resulting structure in the `CMakeLists.txt`.
In our example, we will use one header and .cpp file for the MQTT source.

```
nes-plugins/
├── Sources/
│   ├── MQTTSource/
│   │   ├── MQTTSource.hpp
│   │   ├── MQTTSource.cpp
│   │   ├── CMakeLists.txt
│   │   └── ...
│   ├── KafkaSource/
│   └── ...
├── Sinks/
├── Functions/
└── ...
```

## 2. Dependencies & CMake

Each source plugin builds one implementation library and registers entries with two runtime registries:
1. `Source` — instantiates the source at runtime on workers.
2. `SourceValidation` — validates the configuration during query planning in `nebuli` (our stateless coordinator).

In the `CMakeLists.txt` file of our directory, we tell CMake how we want our plugin to be built:

```cmake
include(${PROJECT_SOURCE_DIR}/cmake/RuntimeRegistrationUtil.cmake)

# One implementation library serving all registries this plugin participates in.
add_library(mqtt_source_plugin_library STATIC MQTTSource.cpp)
target_link_libraries(mqtt_source_plugin_library PRIVATE nes-sources)
target_include_directories(mqtt_source_plugin_library PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/)

find_package(PahoMqttCpp CONFIG REQUIRED) # <-- We depend on the paho-mqtt lib
target_link_libraries(mqtt_source_plugin_library PRIVATE PahoMqttCpp::paho-mqttpp3-static)

link_plugin_library(nes-sources mqtt_source_plugin_library)
add_registry_entry(Source MQTT)
add_registry_entry(SourceValidation MQTT)
```
Notes:
- The plugin name `MQTT` must match the C++ type: the registries derive the type `MQTTSource` and the
  header `MQTTSource.hpp` from `${PLUGIN_NAME}Source`. It is also the key under which the source is
  looked up in the registry.
- The library name is freely chosen;
- Use `FetchContent` for third-party dependencies not already included in our internal `vcpkg/vcpkg.json`.

For a detailed explanation of the plugin system, CMake macros, and how registries work, see `guide/extensibility.md`.
We will look into the construction/validation more closely in the next section.

## 3. Validation & Construction
During lowering of a query plan that uses a source, NebulaStream will ask the `SourceRegistry` for an instance of our source.
The registry entry is a factory that the build generates from the entry template declared in `nes-sources/CMakeLists.txt`; for `add_registry_entry(Source MQTT)` it expands to `makeSourceFactory<MQTTSource>()` (see `SourceRegistry.hpp`).
For this to work, the MQTT source needs to have a constructor that takes a `SourceDescriptor` as an argument, which contains all the configuration parameters passed from the frontend.
Example constructor usage:
```c++
MQTTSource::MQTTSource(const SourceDescriptor& sourceDescriptor)
    : serverURI(sourceDescriptor.getFromConfig(ConfigParametersMQTTSource::SERVER_URI))
    , clientId(sourceDescriptor.getFromConfig(ConfigParametersMQTTSource::CLIENT_ID))
    , topic(sourceDescriptor.getFromConfig(ConfigParametersMQTTSource::TOPIC))
    , qos(sourceDescriptor.getFromConfig(ConfigParametersMQTTSource::QOS))
    /// ...other parameters    
{
}
```
The `getFromConfig` function extracts and converts configuration parameters stored in the `DescriptorConfig`.
At the point of the construction of the source, we expect that all mandatory parameters exist in the config and contain valid values or sensible defaults of the correct types.
In order to give this guarantee, during query planning, we perform a validation phase that will fail in the presence of any wrong or missing parameters.
```c++
DescriptorConfig::Config MQTTSource::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersMQTTSource>(std::move(config), NAME);
}
```

The `SourceValidation` registry entry — `makeSourceValidator<MQTTSource>()`, see `SourceValidationRegistry.hpp` — delegates to this function, so every source class must define it as a static member with exactly this signature:
```c++
static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);
```
The function takes the raw string parameters supplied by the user and is expected to:
- Lookup each parameter name to check if it has been supplied by the user 
- If it does not exist, either report an error or supply a sensible default
- Parse the string into the correct type expected by the source
- Check its validity, e.g., a port needs to be a valid `uint16_t`
 
Let's look at an actual example:
```c++
static inline const DescriptorConfig::ConfigParameter<uint32_t> PORT{
    "SOCKET_PORT",
    std::nullopt,
    [](const std::unordered_map<std::string, std::string>& config)
    {
        const auto portNumber = DescriptorConfig::tryGet(PORT, config);
        if (portNumber.has_value())
        {
            constexpr uint32_t PORT_NUMBER_MAX = 65535;
            if (portNumber.value() <= PORT_NUMBER_MAX)
            {
                return portNumber;
            }
            NES_ERROR("Specified port is: {}, but ports must be between 0 and {}", portNumber.value(), PORT_NUMBER_MAX);
        }
        return portNumber;
    }};
```
Here, we first use the `tryGet` function of the `DescriptorConfig` that returns an optional.
If the parameter is set, we move on to the port number validation, otherwise we just return `std::nullopt`.
We check if the port number is within the allowed bounds and return it if yes, an empty optional otherwise.

## 4. Implementation

The interface to implement a source is straightforward:

```c++
/// Read data into a `TupleBuffer`, until the TupleBuffer is full, or stop was requested.
/// @return the number of bytes read
virtual size_t fillTupleBuffer(NES::TupleBuffer& tupleBuffer, const std::stop_token& stopToken) = 0;

 /// If applicable, opens resources like file descriptors, database connections, etc.
virtual void open() = 0;

/// If applicable, closes resources like file descriptors, database connections, etc.
virtual void close() = 0;
```

The central method we need to implement is called `fillTupleBuffer`, which takes a reference to a buffer and a `stop_token`.
It should implement the necessary logic to ingest data into the buffer.
Under normal circumstances, on each invocation, you should write to the beginning of the buffer's memory.

We return from this function in either of four cases:
1. An error internal to our source happens, e.g., the MQTT source could not establish a connection to the specified broker2. 
2. The `stop_token` indicates stop has been requested from a higher-level component (e.g., the query was stopped)
3. We have received an "end of stream" (EOS) signal
4. The buffer has been filled to its capacity

In case (1), we can decide whether the error is recoverable or unrecoverable, and either try again or throw an
exception.
When stop has been requested (2), we should return, signalling the number of bytes read so far.
In order to be responsive to possible stop requests, we should check the `stop_token` regularly, if possible.
(3) indicates an EOS signal (e.g., a terminated connection) and it means that we do not expect any further data, which
we report by returning 0.
Finally, if buffer has been filled entirely (4), we should also return, reporting the buffer size as the number of read
bytes.

The `open` and `close` methods can be used to create or release resources like file descriptors.
`open` will be called once before the first invocation to `fillTupleBuffer`, `close` likewise at the end, before
dropping the source.

For our MQTT source, `open` might look like this:
```c++
void MQTTSource::open()
{
    client = std::make_unique<mqtt::client>(serverURI, clientId);
    try
    {
        const auto connectOptions = mqtt::connect_options_builder().automatic_reconnect(true).clean_session(false).finalize();
        client->start_consuming();

        const auto token = client->connect(connectOptions);
        if (const auto response = token->get_connect_response(); !response.is_session_present())
        {
            client->subscribe(topic, qos)->wait();
        }
    }
    catch (const mqtt::exception& exception)
    {
        /// Convert an MQTT exception to internal exception
        throw CannotOpenSource("Could not connect to MQTT broker: {}", exception.what());
    }
}
```

In short, we instantiate an MQTT client through which we can interact with a broker, passing necessary information.
Then, we try to establish a session and subscribe to a topic, indicating that we are ready to start consuming data.
If the client throws any exception, we rethrow a `CannotOpenSource` exception, which is a NebulaStream-specific exception.
A list of all NebulaStream-specific exceptions can be found in `ExceptionDefinitions.inc`.

Similarly, close can be implemented: 
```c++
void MQTTSource::close() try 
{
    client->unsubscribe(topic)->wait();
    client->disconnect()->wait();
} 
catch (const mqtt::exception& e)
{
    NES_ERROR("Error on close: {}", e.what());
}
```
Because no methods of the `MQTTSource` will be called after `close`, we simply report any MQTT-related error closing the session and return.

In most scenarios, `fillTupleBuffer` implements an I/O request loop that stops when either the buffer is full, an error occurs or EOS is signalled.
```c++
size_t MQTTSource::fillTupleBuffer(NES::TupleBuffer& buf, const std::stop_token& stopToken)
{
    /// (1) Setup offset within buffer and size of buffer
    size_t offset = 0;
    const size_t size = tupleBuffer.getBufferSize();

    /// (2) Process stashed payload first (from prior invocation of `fillTupleBuffer`)
    if (!payloadStash.empty())
    {
        auto payload = payloadStash.consume(size);
        writePayloadToBuffer(payload, buf, offset);
    }

    /// (3) Fill buffer with new messages until full, timeout, or Error/EoS
    while (offset < size && !stopToken.stop_requested()) /// <-- if stop requested, end loop
    {
        if (auto& message = client->consume(); message) 
            writePayloadToBuffer(message->get_payload(), buf, offset); /// <-- message received successfully
    }
    return std::min(offset, size);
}
```
A thing to notice here is that this MQTT client library allocates its own receive buffer for incoming data.
This possibly results in larger messages than currently available space in our buffer.
To handle such a case, we stash intermediate data until the next invocation of `fillTupleBuffer`, where it is consumed before making another consume request.
When errors occur during ingestion, the client will throw an exception, which is handled in a higher-level component.
If you want to handle an error that is recoverable, you should catch the exception in this method and call any code required for recovery.

## 5. Testing
Currently, there is no way to test sources easily, especially ones that require external systems to be booted up.
However, you can write unit tests if you require any additional logic that does not revolve around I/O.
In the near future, we plan to integrate a toolkit for containerized testing of sources/sinks that interact with external systems.
