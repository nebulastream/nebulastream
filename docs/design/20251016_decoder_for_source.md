# The Problem
## Motivation
Currently, NebulaStream is able to receive data in the `formats` CSV, JSON and Native, more formats like XML are likely going to be added in the future. The component responsible for detecting tuples in the raw buffers filled with data in these formats is the `nes-input-formatter`. 

However, we currently do not consider codecs when treating incoming data. Data of any format can be encoded according to the rules of a certain codec. Exemplary of this are general purpose `compression codecs` like Facebook's "Zstandard" (Zstd)[1] or Google's "Snappy" [2], which are able to compress and decompress data byte per byte, independent of the data type or format. 
`Protobuf`, as a serialization format, could also be considered a codec.

Codecs, especially compression codecs, enable multiple use-cases in NebulaStream: 
Transmitting compressed data between workers utilizes available bandwidth more efficiently, potentially mitigating the bottleneck that low bandwidth can cause.
It enables efficient `state persistency`, Apache Flink for example uses general purpose compression to encode checkpoints [6].
Furthermore, after implementing light-weight and general purpose compression codecs, we could utilize formats like the columnar `Apache Parquet` [4], which cascade codecs for a high compression ratio, as internal storage format.

## Problem of current Implementation
The mentioned motivations can be narrowed down to the following problem:

**P1** We currently have no means of `significantly reducing data size` or handling compressed, incoming data but would definitely benefit from it considering the size of our systest datasets and the problem of low bandwidth.

Since formats and codecs seem similar at first, one might consider implementing the decoders as `input-formatter indexers`. However, encoded data might have properties that make this solution not feasible:

**P2** Many compression codecs such as `LZ4`[3] need the data to arrive `in-order` for incremental decoding, since encoded data usually reference reoccurring byte patterns via pointer to previous raw bytes. However, the `input-formatter` component already receives the buffers `out-of-order`.

**P3** Especially general purpose codecs produce encoded data of `any underlying format`.
If we were to implement decoders as indexers, we would therefore have to create an indexer implementation `per format` for a new codec (like SnappyCSV, SnappyJSON) which hinders extensibility.

# Goals
To solve the described problems, a nes-codec component with the following properties should be added:

**G1** Decoders for codecs. 
For each codec, that we decide to support, a decoding function should be provided.
It should be able to decode tuple buffers incrementally since we operate in a data stream context and usually do not receive one encoded block at a time.
(**P1, P3**)

**G2** In-order decoding. 
The decoding functions assume that the tuple buffers arrive in-order.
Thus, tuple buffers should be decoded in a component, where the system still obtains them in-order.
Except for the decoding itself, the integration of the decoder should ideally not cause any additional work like ordering of the buffers or waiting for missing buffers.
(**P2, P3**)

**G3** Easily extensible.
Adding a decoder should only require the addition of the decoding function, without changing any other components of the system.
This way, adding support for a new codec or format with codecs is straightforward. 
(**P1, P3**)

# Non-Goals
**NG1** Encoders for codecs.
The primary focus of this design document is the decoding of incoming data before the input formatting process. 
Encoding the query results before emitting them affects different components than decoding.
The benefits and challenges of this practice should be elaborated upon in a separate design document.

**NG2** Decoding and encoding during query execution.
Sending compressed data between pipelines and only decompressing the data if an operation in the pipeline demands it could increase the throughput. 
However, such a feature would require changes of many components such as the query plan optimizer, operators and also the implementation of **NG1**.  

# Solution Background
We conducted an experiment regarding the improvement of throughput for TCP data sources on the branch `benchmark-ingestion-decoding-experiment`.
On this branch, we implemented a TCP source that expects LZ4 encoded data. The `fillBuffer` method of this source in `nes-sources/src/Blocking/Sources/BlockingTCPLZ4Source.cpp` decodes the received compressed bytes incrementally before writing them into the tuple buffer.

For the experiment, we connected the rust server at `testing/rustE2EBenchmarking/src/tcp_server.rs` to the worker and let it send the LZ4-encoded data with a constant sending rate of 125mb/s.
We used the `ysb1k_more_data_3GB` data set, which was compressed to the size of 937mb by the compression algorithm.

For queries using only stateless operators like selections and projections, the worker was able to handle the new decoding overhead without reducing the 125mb/s sending rate, effectively tripling the throughput for these queries.

While the implementation of the decoder on this branch was a mere prototype and does not match the proposed solution, the results of the experiment show that including decoders for compression codecs comes with benefits.

Furthermore, we encoded every systest dataset with LZ4, Zstd and Snappy to evaluate the compression ratio. 
We discovered that especially Zstd achieves high compression ratios, not rarely reducing a dataset to less than 20% of its original size. 
The exact sizes of each encoded dataset can be found on the table at [7].


# Our Proposed Solution
For our solution, we propose decoding the encoded data in the `nes-sources` component. 
Currently, each physical source instance is handled by its own `source thread`, which emits the filled tuple buffers in the source thread routine. 
Before emitting a buffer, the thread must decode its contents, should the source utilize a codec. 
This way, we can guarantee that the decoder receives the data in-order, achieving **G2**.

## The abstract Decoder class
To implement decoders for codecs, like **G1** demands, we propose an abstract `Decoder` class with a simple interface.
Each implementation of the Decoder class for a codec must implement the function `decodeAndEmit()`.
```c++
 virtual void decodeAndEmit(
        TupleBuffer& encodedBuffer,
        TupleBuffer& emptyDecodedBuffer,
        const std::function<std::optional<TupleBuffer>(const TupleBuffer&, const DecodeStatusType)>& emitAndProvide)
        = 0;
```
This function obtains a single tuple buffer filled with encoded data and an initial empty tuple buffer. It should decode the contents of the `encodedBuffer` argument and write the decoded data in the `emptyDecodedBuffer`.

Afterward, the third argument, the `emitAndProvide()` function, should be called. Its purpose is to emit the decoded buffer to the input-formatter. 
Alongside the decoded buffer, a `DecoderStatusType` instance is passed to `emitAndProvide()`. This is an enum class, defined in the `Decoder` class, and consists of two status:
```c++
 enum class DecodeStatusType : uint8_t
    {
        FINISHED_DECODING_CURRENT_BUFFER,
        DECODING_REQUIRES_ANOTHER_BUFFER
    };
```
`FINISHED_DECODING_CURRENT_BUFFER` signalizes, that the encoded tuple buffer was fully decoded. 
If this status is passed to `emitAndProvide()`, it simply emits the decoded buffer.

`DECODING_REQUIRES_ANOTHER_BUFFER` signalizes, that the encoded buffer was not fully decoded yet and that another empty buffer is required to hold decoded data. 
In this case, `emitAndProvide()` must return a new, empty tuple buffer and the decoding continues.

We decided that decoders should not have full access to the `BufferProvider`. Instead, with `emitAndProvide()`, the decoder can obtain an additional empty buffer in exchange for a filled, decoded buffer.

Since the decoding happens incrementally, the decoder implementations store `decoding context` information about headers, previous data etc. in class members. 
How exactly this context is managed, differs between implementations.

## DecoderRegistry

To manage the decoder implementations, we propose the usage of a `Registry`. 
This allows us to add new decoder implementations as optional plugins in the `nes-plugin` component, contributing to the extensibility that **G3** demands. 

The `provideDecoder()` function, which is responsible for creating decoder instances, currently does not obtain any arguments besides the name of the decoder, since the solution was developed with `general purpose compression codecs` in mind, which do not require any configurations.
```c++
std::unique_ptr<Decoder> provideDecoder(std::string decoderType);
```

Should future decoder implementations require information about the data types or user configurations, we could add a `decoder configuration` map and the `Schema` to the `DecoderArguments`.

## Integration in nes-sources

To define the codec that a source utilizes, we propose the addition of a `codec` config parameter to the source descriptor configuration.

```c++
static inline const DescriptorConfig::ConfigParameter<std::string> CODEC{
        "codec",
        "None",
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(CODEC, config); }};
```
Since no codec is used per default, we do not need to change the source configurations for any preexisting source tests in our system. 
Adding codec as config parameter does not require changes of the SQL parser, which makes for a tidy integration into the system.

If a codec option other than `None` is set, the `SourceProvider` calls `provideDecoder()` to obtain a unique pointer to the decoder instance for the source.
The decoder pointer is then moved into the `SourceHandle` constructor as `optional argument`, which will move it to the `SourceThread` constructor as `optional argument`.

The `SourceThread` stores the decoder pointer as optional member. When `SourceThread::start()` is called, the decoder pointer is passed as optional argument to the `dataSourceThreadRoutine()` function.
In the thread routine, after filling a tuple buffer, we check if a decoder argument was provided. 
If not, the filled tuple buffer is emitted like in the current state of the system. Otherwise, we provide an initial empty tuple buffer for the decoded data, a lambda function as `emitAndProvide` argument and call the `decodeAndEmit()` method of the provided decoder.
```c++
if (decoder.has_value())
            {
                /// Lambda function to emit a decoded buffer and optionally provide an empty, new one.
                auto emitAndProvide = [&emit, &bufferProvider](
                                          const TupleBuffer& filledDecodedBuffer,
                                          const Decoder::DecodeStatusType decodeStatus) -> std::optional<TupleBuffer>
                {
                    /// Emit the filled buffer.
                    emit(filledDecodedBuffer, true);
                    /// If required, provide a new, empty buffer.
                    return decodeStatus == Decoder::DecodeStatusType::DECODING_REQUIRES_ANOTHER_BUFFER
                        ? std::optional(bufferProvider.getBufferBlocking())
                        : std::nullopt;
                };

                /// Get an initial empty buffer for the decoded data
                auto decodedBuffer = bufferProvider.getBufferBlocking();
                decoder.value()->decodeAndEmit(emptyBuffer, decodedBuffer, emitAndProvide);
            }
            else
            {
                emit(emptyBuffer, true);
            }
```

While the addition of decoders in general affects the `SourceDescriptor`, `SourceProvider`, `SourceHandle` and `SourceThread` components of `nes-sources`, adding a decoder implementation for a new codec only requires the implementation of `decodeAndEmit()` in a new decoder subclass as plugin, which makes extensions straightforward.
Thus, **G3** is achieved.

# Proof of Concept
On the branch [PoC-decoders-for-source](https://github.com/nebulastream/nebulastream/tree/PoC-decoders-for-source), the proposed solution was implemented. 


The `Decoder` abstract class and a `LZ4Decoder` implementation, as well as the `DecoderProvider` and the `DecoderRegistry` can be found in [nes-codecs](https://github.com/nebulastream/nebulastream/tree/PoC-decoders-for-source/nes-codecs). 
In [nes-plugins](https://github.com/nebulastream/nebulastream/tree/PoC-decoders-for-source/nes-plugins), two additional decoders for the `Snappy-Framing`[5] and `Zstd` codecs are implemented.

# Alternatives
## A1 Integrating the decoders in nes-input-formatters
In this alternative solution, the decoding of the tuple buffers is moved to the input formatter component and must be performed before the tuples and fields of the buffers are determined.

This approach keeps the source itself as simple as possible by avoiding the two separate options of emitting tuple buffers that our proposed solution adds.

However, the data does not arrive in-order at the input formatter. Since compression codecs like `LZ4` and `Zstd` require the data to be decoded in-order, we have two options:
- We do not support codecs that require in-order decoding, ruling out codecs like `Lz4` and `Zstd` and disregarding **G2**.
- We must wait for the tuple buffer with the lowest sequence number to continue decoding. 
This could cause slow-downs and also contradicts the asynchronous out-of-order approach of the input-formatter.


# Summary
In this design document, we proposed a solution for decoder support in NebulaStream. 

To use limited bandwidth more efficiently and to enable efficient fault tolerance and potential internal storage formats in the future, a decoder component should be added to the system.

Our proposed solution adds a `Decoder` abstract class with a simple decoding interface.
Implementations for the decoder can be added as plugins with the help of the `DecoderRegistry`, making the component easily extensible without affecting other system components.

We propose to decode the encoded tuple buffers in the source thread routine. 
This provides the advantage that the tuple buffers naturally occur in the correct order in this part of the system, thus avoiding waiting time for missing tuple buffers.
Decoding in the source thread therefore seems like the best alternative, since the speed of the input formatter depends on being able to process the buffers out of order.

# Open Questions
- How will we integrate decoding, if we have a certain number of threads available to handle all sources instead of one source thread per source?
Since this feature is not implemented yet either, proposing a solution for this might be too early. 

# Sources and Further Reading
[1] https://github.com/facebook/zstd

[2] https://github.com/google/snappy

[3] https://github.com/lz4/lz4

[4] https://parquet.apache.org/docs/file-format/

[5] https://github.com/google/snappy/blob/main/framing_format.txt

[6] https://nightlies.apache.org/flink/flink-docs-masterdocs/ops/state/large_state_tuning/#compression

[7] https://docs.google.com/spreadsheets/d/1V_6wUDSKYtx51CS7hssRD_hR5veEw3AOnpYKd9kX82Y/edit?usp=sharing
