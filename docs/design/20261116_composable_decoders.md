# Composable Decoders in the Source Thread

Task: [#2186](https://github.com/nebulastream/nebulastream/issues/2186) (epic [#2184](https://github.com/nebulastream/nebulastream/issues/2184)).
This document extends the design document by Leo Raasch
([`20251016_decoder_for_source.md`](https://github.com/nebulastream/nebulastream/blob/dd-decoder-for-codecs/docs/design/20251016_decoder_for_source.md),
PR #1190, issue #1188), which covers a single decompression codec, to an ordered chain of decoder stages.
Sections marked *(adopted)* are taken from that document with minor edits; everything else is new or changed.

# Scope and Terms
This document covers **decoding** only: transforming the encoded byte stream that a source reads from an external system
into the byte stream that the source's input formatter parses. It does not cover encoding, the input formatter itself,
or the semantics of the end of a stream (see the non-goals).

**Codec.** A data representation, e.g. a compression format, an encryption scheme or a serialization format. A
**decoder** transforms data from one codec into another; a **decoder stage** is one such transformation within a
source, implemented as a plugin.

**Assumption: in-order arrival.** A source reads the bytes of its stream in the order in which they were produced (as
for files and TCP). Buffers that arrive out of order and must first be reordered, e.g. by Kafka offset or by a
NebulaStream sequence number, are not considered (NG7).

## Kinds of decoders
The kinds differ in what a stage must remember between two input buffers. For each kind, two examples with different
properties:

**Decompression** inverts a compression codec; the output is in general larger than the input, by an unbounded ratio.
- *Zstd* (stateful): a block may reference previously decoded output up to the window size (by default up to 128 MB),
  and a block may span several input buffers. The stage keeps the window and the incomplete block across calls; the
  zstd library holds both internally.
- *Snappy, framing format* (independent units): every chunk (at most 64 KB uncompressed) is compressed independently of
  the preceding chunks. Apart from a chunk that spans a buffer boundary, the stage keeps no state between calls.

**Decryption** inverts an encryption scheme; the output has about the size of the input.
- *AES in counter mode (CTR)* (stateless per byte): byte *i* of the plaintext depends only on the key, the nonce and its
  offset *i* in the stream. Apart from the current offset, the stage keeps no state, and every input buffer can be
  decrypted and passed on immediately.
- *Chunked authenticated encryption, e.g. AES-GCM per record* (intermediate data): the plaintext of a record may only be
  released after the record's authentication tag at its end has been verified. The stage must keep an entire record,
  which may span several input buffers, before it can emit any of it.

**Deserialization** converts a serialization format into the records or fields of the schema; it borders on the work
of the input formatter (NG3).
- *Avro object container file* (stateful): the file header carries the writer's schema and a sync marker, which the stage
  must keep for the whole stream to decode the subsequent blocks.
- *Fixed-width binary records with a schema known in advance* (nearly stateless): every record has the same size and
  layout. The stage keeps at most the bytes of one record that spans a buffer boundary.

# The Problem

## Motivation *(adopted)*
NebulaStream ingests data in the formats CSV, JSON and Native; the `nes-input-formatters` detect tuples and
fields in the raw buffers a source fills. Incoming data, however, is frequently *encoded*: compressed by a
general-purpose codec (Zstd, LZ4, Snappy), encrypted, or wrapped in a serialization or container format.
General-purpose codecs operate byte-wise and are independent of the underlying format.

Supporting codecs makes such data ingestible at all, uses limited network bandwidth more efficiently, and
shrinks the systest and benchmark datasets. Leo Raasch measured that LZ4 decoding in a TCP source tripled the
effective throughput of stateless queries at a fixed 125 MB/s link, and that Zstd reduces the systest data to
21 % of its size at about 3 % additional execution time (numbers in #2184).

## Problems
**P1** *(adopted)* NebulaStream has no means to ingest encoded data or to reduce the size of the data it reads.

**P2** *(adopted)* Most codecs require their input **in order**: a compressed stream references earlier bytes,
a cipher carries state, frames span buffer boundaries. Buffers reach the input formatter **out of order**. Decoding
there would require reordering the buffers and processing them sequentially, which adds complexity and serialises the
otherwise parallel formatting. Decoding should therefore not happen in the input formatter.

**P3** *(adopted)* A general-purpose codec can wrap any format. Implementing codecs as input-formatter
indexers would require one indexer per pair of codec and format (SnappyCSV, SnappyJSON, ...).

**P4** Streams are often encoded by **several** codecs in sequence, e.g. serialized, then compressed, then
encrypted. The decoder must apply the inverse sequence. Which codecs occur and in which order is determined by
the producer, not by NebulaStream. A design for a single codec does not express this.

**P5** Decoding is **n:m** in buffers, and the ratio is not bounded: one 4 KB buffer of highly compressible input
can expand to thousands of output buffers, and one frame can span many input buffers. A decoder that takes
output buffers from the pool without limit can exhaust the pool shared by all queries of the worker.

# Goals
**G1** *(adopted, extended)* **Decompression and decryption.** Each supported codec provides an incremental decode
function for a stream that arrives in buffers. The interface supports decompression and decryption stages. (P1, P3)

**G2** *(adopted)* **In-order decoding.** Decoding runs where buffers are still in order, without additional
reordering or waiting, under the assumption of in-order arrival. (P2)

**G3** *(adopted)* **Extensible.** Adding a codec means adding a plugin; no other component changes. (P1, P3)

**G4** **Bounded resources.** The number of buffers a decoder chain holds is bounded independently of the
compression ratio, and every blocking point of a stage can be interrupted when the query stops. (P5)

**G5** **No cost without a decoder.** A source without a decoder executes the current code path. (P1)

**G6** **Testing.** Every plugin has unit tests. In addition, ConnTest is extended to sources with a decoder: the data
sent to the source is encoded, a reference implementation of the codec (the oracle) decodes the same bytes, and the
output of the source is compared with the output of the oracle (see [Testing](#testing)). (P1)

**G7** **First codecs.** LZ4 and Zstd (decompression) and AES-CTR (decryption, with the key as a configuration option,
NG8) are implemented as plugins on the new interface, with micro benchmarks of their decoding throughput. In addition, a proof-of-concept plugin based on libffmpeg (libavcodec) decodes a video stream, to
show that the interface is not limited to general-purpose byte compression; it is developed in a separate branch as
part of the exploration of further decoders ([#2188](https://github.com/nebulastream/nebulastream/issues/2188)). (P1)

**G8** **Composable and configurable.** A source runs an ordered chain of any number of stages, in any order. A user
declares the chain and the options of each stage, and invalid chains are rejected before a query runs. (P4)

# Non-Goals
**NG1** *(adopted)* **Encoding.** Encoding on the sink side has different placement trade-offs (#2185, #2190).

**NG2** *(adopted)* **Decoding during query execution.** Keeping data compressed between operators requires
optimizer and operator changes.

**NG3** **Deserializers.** The first iteration does not support deserializing stages (e.g. Avro, Protobuf). They require
a definition of the boundary between decoder and input formatter, including shared parsing code with
`nes-input-formatters`, which is the subject of [#2188](https://github.com/nebulastream/nebulastream/issues/2188). In this
iteration, a stage outputs bytes, and the input formatter produces tuples.

**NG4** **Codecs beyond those of G7** (Snappy, gzip, ...). They follow as further plugins.

**NG5** **Integration with the async IO runtime (#2154).** Open questions are listed at the end of this document.

**NG6** **End-of-stream semantics.** A stream can end inside a frame (truncation), a problem that sources already have
independently of decoders (e.g. a JSON document whose tail is missing). Whether and how such a stream fails the query is
approached separately for all sources; this document only defines when a stage is told that its input has ended.

**NG7** **Out-of-order arrival.** Sources whose buffers arrive out of order (e.g. Kafka offsets, NebulaStream sequence
numbers) require reordering before decoding. This is a potential next step.

**NG8** **Key management.** Securely storing and distributing private keys for decryption stages is a separate problem.
How a decryption stage obtains its key beyond a configuration option is out of scope.

# Solution Background
- Leo Raasch's design document and proof of concept (branch `PoC-decoders-for-source`, `nes-codecs/`, LZ4, Zstd
  and Snappy plugins) and the TCP+LZ4 bandwidth experiment (`benchmark-ingestion-decoding-experiment`). The codec
  loops of the PoC are reused. Porting them to the composable interface requires four adjustments, which follow
  from the goals of this document rather than from the scope of the PoC: decoding all frames contained in one
  input buffer (the Snappy decoder handles one frame per call), reporting decode errors as query failures (the
  PoC logs them), detecting the end of a Zstd stream from the frame structure, and obtaining buffers through
  stop-aware waits (G4).
- The registry pattern of sources, sinks and input formatters (`create_runtime_registry`, `add_registry_entry`)
  and their config schemas.

# Our Proposed Solution

## Overview
A source optionally owns a decoder chain: an ordered list of `DecoderStage` instances, created from plugins. The
source thread (`dataSourceThreadRoutine`, `nes-sources/src/SourceThread.cpp`) reads the stream in order and passes
every filled buffer to the first stage. Each stage emits its output buffers to the next stage; the last stage emits
to the source's `emit`, from where buffers go to the input formatter. All stages are treated alike: no stage has a
fixed kind or position, and the last stage is an ordinary stage whose output is a format the source's input
formatter parses. Between stages, data is passed in tuple buffers from the source's buffer pool.

```
fillTupleBuffer ──► stage 1 ──► stage 2 ──► ... ──► stage n ──► source emit ──► input formatter (scan)
   (raw bytes)                                                  (sequence numbers,
                                                                 inflight limit)
```

## Plugin structure
A decoder plugin consists of the following parts (the LZ4 plugin is the reference):

```
nes-plugins/Decoders/LZ4Decoder/
├── CMakeLists.txt        # find_package(lz4), add_plugin_library, add_registry_entry(DecoderStage LZ4) ...
├── LZ4Decoder.hpp/.cpp   # the stage and its config
└── tests/                # unit tests
```

```c++
/// Options of the stage. NAME is the identifier used in a decoder chain; the schema lists the options with
/// their types, defaults and validation, like the config of a source or input formatter.
struct LZ4DecoderConfig
{
    static constexpr std::string_view NAME = "LZ4";
    static inline const auto schema = createConfigSchema(Identifier::parse(NAME) /*, options ... */);
};

class LZ4Decoder final : public DecoderStage
{
public:
    explicit LZ4Decoder(const InstantiatedConfig& config);   // created per source, per query
    std::expected<void, Exception> decode(const TupleBuffer& input, TupleBuffer output, DecoderContext& context) override;
    std::expected<void, Exception> flush(TupleBuffer output, DecoderContext& context) override;
private:
    /* codec state, e.g. LZ4F_dctx */
};
```

The plugin registers the stage under `NAME` in the `DecoderStageRegistry` and its schema in the
`DecoderStageConfigSchemaRegistry` (CMake `add_registry_entry`). Plugins with an external library (lz4, zstd) can be
disabled in CMake like other optional plugins. Nothing outside the plugin directory changes (G3).

## The stage interface
```c++
class DecoderStage
{
public:
    virtual ~DecoderStage() = default;

    /// Decodes the next buffer of the stage's input stream into `output`, an empty buffer provided by the chain.
    /// Full output buffers are exchanged for empty ones through the context; the last output buffer of the call is
    /// passed on with `emitLast`. Returns an error for input that cannot be decoded.
    virtual std::expected<void, Exception> decode(const TupleBuffer& input, TupleBuffer output, DecoderContext& context) = 0;

    /// End of the input stream: passes on remaining output. Returns an error if the stage cannot complete its output.
    virtual std::expected<void, Exception> flush(TupleBuffer output, DecoderContext& context) { return {}; }
};

/// The only way a stage passes on buffers and obtains new ones.
class DecoderContext
{
public:
    /// Passes a full output buffer on and returns an empty one in exchange. std::nullopt once the query is stopping,
    /// in which case the stage returns without further work.
    std::optional<TupleBuffer> emitAndGetNext(TupleBuffer filled);

    /// Passes the last output buffer of the current call on, without obtaining a new one.
    void emitLast(TupleBuffer filled);
};
```

A stage has no access to the buffer pool. As in Leo Raasch's `emitAndProvide`, the chain provides one empty output buffer
with every input, and the stage obtains further buffers only by handing over a full one. A filled buffer carries its
number of valid bytes as its number of tuples, as the raw buffers sources fill. Passing a buffer on means: stage *i*
calls `decode` of stage *i+1* with it, and the last stage calls the source's `emit`. A stage implementation therefore
does not depend on its position in the chain.

**Buffers per stage.** A stage holds at most two buffers at a time, one input and one output, and both are exchanged:
- the **input** buffer is held for the duration of `decode` and given back on return, in exchange for the next input
  with the next call;
- the **output** buffer is provided with the call and exchanged with `emitAndGetNext` whenever it is full. When the
  input is consumed, the stage passes its last, possibly partially filled, output buffer on with `emitLast`. If the
  call produced no output, the stage does not pass the buffer on, and it returns to the pool.

Since the output of stage *i* is the input of stage *i+1*, a chain of *n* stages holds at most *n + 1* buffers: the
buffer the source filled and one output buffer per stage. This follows from the interface; no buffer budget is
configured.

A stage guarantees:
1. It processes every buffer of its input in stream order. `decode` is called from one thread at a time, so a stage
   need not be thread-safe.
2. It writes output only into the buffers it receives from the chain.
3. It passes on all output that can be derived from an input buffer before `decode` returns, and keeps no buffer
   across calls.
4. It returns `CannotDecodeSourceData` with the stage name and the cause for input that cannot be decoded, instead of
   throwing. The chain stops, and the source fails through its existing error path with this error.
5. It returns when `emitAndGetNext` returns `std::nullopt`.

**Advantages of this interface:**
- The memory of a chain is bounded by its number of stages, independent of the compression ratio, without a
  configuration parameter.
- A stage cannot take more buffers than its bound, because it has no access to the pool.
- Output is passed on as soon as a buffer is full, so the source's inflight limit applies during decoding, and all
  output of an input is passed on when that input is consumed, so decoded data reaches the query without delay.
- Stop handling is implemented once, in the chain, instead of in each plugin; later additions (e.g. counters for
  decoded bytes) extend the context instead of the signature of `decode`.

**Disadvantages:**
- Stages are coupled: while a later stage or the source's `emit` waits, earlier stages cannot work ahead.
- A stage cannot keep buffers across calls; units that span input buffers must be held by the codec library or copied
  into the stage's own state.
- Every input produces at least one partially filled output buffer, so small inputs lead to many small buffers.
- The chain obtains an output buffer for every call, also for calls that produce no output.

## Stage state
*(adopted)* Since decoding happens incrementally, a stage stores its decoding context (headers, previous data, codec
state) in members of its class. Stages are processed in order, so a stage sees each byte of its input exactly once and
after all preceding bytes. Its state is limited to what is needed to decode the next input in order:
- the codec context, owned by the codec library (e.g. `LZ4F_dctx`, `ZSTD_DCtx`), including the history window and
  incomplete blocks the library buffers internally;
- metadata about the stream position, e.g. whether the stream is currently inside a frame, which `flush` can use to
  detect truncation (NG6).

A stage holds no input or output buffers across calls (guarantee 3). This suffices for LZ4 and Zstd, whose
streaming APIs consume any prefix of the input and keep incomplete blocks internally. Stages that must retain input
buffers until a later buffer arrives are not supported in this version.

All stage kinds (decompression, decryption, ...) implement the same `DecoderStage` interface; they differ only in their
configuration and in the state they keep, and both are handled by the plugin. The framework does not manage stage
state. Framework-managed state becomes relevant once stages must be suspended and
resumed outside a dedicated thread, as on the async IO runtime (see the open questions).

## Chain execution
A chain is a `Decoder` object owned by the source thread. It holds the stage instances in decoding order and one
`DecoderContext` per stage. Everything below runs on the source thread; there are no queues and no other threads.

**Source thread loop.** The decoder is inserted between `fillTupleBuffer` and `emit` of `dataSourceThreadRoutine`
(`nes-sources/src/SourceThread.cpp`):
```
loop until stop requested:
    backpressureListener.wait(stopToken)            // sink pressure pauses the source here
    raw = pool.getBufferUnlessStopped()             // the source's input buffer
    result = source.fillTupleBuffer(raw)            // read from the external system
    if result is end of stream:
        if decoder: decoder.flush()                 // error: fail the source
        emit final watermark and EoS; return
    raw.setNumberOfTuples(result.numberOfBytes)
    if decoder: decoder.decode(raw)                 // returns when all output of raw has been emitted;
                                                    // error: fail the source
    else:       emit(raw)
```

**Inside the chain.** Stage *i* is triggered by a buffer that is passed on to it, either the source's `raw` buffer
(stage 1) or an output buffer passed on by stage *i−1*:
```
runStage(i, input):
    output = pool.getBufferUnlessStopped();  if stopped: return
    return stages[i].decode(input, output, contexts[i])   // an error ends the chain

contexts[i].emitAndGetNext(filled):
    passOn(i, filled)                                // runs all later stages on `filled` first
    return pool.getBufferUnlessStopped()             // std::nullopt if stopped

contexts[i].emitLast(filled):
    passOn(i, filled)

passOn(i, buffer):
    if i is the last stage: sourceEmit(buffer)       // metadata, inflight limit, hand-off to the query engine
    else:                   runStage(i + 1, buffer)

decoder.decode(raw) = runStage(1, raw)
```
`passOn` is a plain function call, so stage *i+1* runs to completion on a buffer before stage *i* continues with its next
output buffer. For a chain `AES → LZ4`, one input buffer is processed as follows:
```
decoder.decode(B0)
└─ AES.decode(B0, B1)                       B1 provided by the chain
   ├─ decrypt B0 into B1
   └─ emitLast(B1)
      └─ LZ4.decode(B1, B2)                 B2 provided by the chain
         ├─ decompress into B2 until full
         ├─ emitAndGetNext(B2) → sourceEmit(B2), returns B3
         ├─ decompress into B3 until full
         ├─ emitAndGetNext(B3) → sourceEmit(B3), returns B4
         ├─ B1 consumed: emitLast(B4) → sourceEmit(B4)
         └─ return
      B1 released
   return
B0 released; the source thread reads the next buffer
```

**End of stream.** `decoder.flush()` calls `flush` on stage 1 with an empty output buffer; everything stage 1 passes on
runs through the later stages as above. Then `flush` is called on stage 2, and so on to stage *n*.

## Backpressure
The decoder adds no backpressure mechanism; it relies on the two that exist:
- **Inflight limit (query engine behind).** The source's `emit` blocks when the source has `maxInflightBuffers`
  unprocessed buffers in the query engine (`nes-query-engine/RunningSource.cpp`). With a decoder, this happens in
  `passOn` of the last stage, so the whole chain waits and holds at most its *n + 1* buffers.
- **Backpressure listener (sink behind).** The source thread waits before the next read. Decoding of the current input
  completes; its output is bounded by the inflight limit.

While the source thread waits, it reads nothing from the external system, whose own flow control then slows down the
producer (e.g. TCP flow control).

## End of stream, failure, stop
- **End of stream.** When the source reports end of stream, the chain calls `flush` on stage 1, then on stage 2, and
  so on to stage *n*. Afterwards the source thread emits the final watermark and EoS. Whether a stream that ends inside
  a frame fails the query is not defined here (NG6).
- **Failure.** An error returned by `decode` or `flush` ends the chain; the source thread fails through its existing
  error path, and the query fails with `CannotDecodeSourceData`.
- **Stop.** When a query stops, the owner of the source thread requests stop on its `std::stop_token` and joins the
  thread, so no wait in the source thread may outlast a stop request. A stage waits only in the context. The chain obtains
  buffers from the pool with a stop-aware wait, and `emitAndGetNext` returns `std::nullopt` on stop; passing a buffer
  on ends in the source's `emit`, which returns on stop (G4).

## Metadata
Intermediate buffers carry no metadata apart from their number of valid bytes, which is stored in the buffer's number of
tuples. Buffers leaving the last stage receive their metadata (origin id, sequence number, chunk number, watermark) in
the source's `emit`, exactly as undecoded buffers do. For now, the decoder adds no further metadata to them.

## Configuration
The source descriptor carries the resolved, ordered list of stages, each as stage type plus validated per-stage
options. The order is the decoding order, i.e. the reverse of the producer's encoding order. Validation at creation
time rejects unknown stage types, options that belong to no stage of the chain, and an empty chain (G8).

A chain is declared as a named decoder and referenced by the physical source; the stages are resolved and copied into
the source descriptor when the physical source is created:
```sql
CREATE DECODER csv_lz4 AS (LZ4);
CREATE DECODER csv_zstd AS (ZSTD) SET (27 AS ZSTD.WINDOW_LOG_MAX);
CREATE PHYSICAL SOURCE FOR s TYPE File SET ('csv_lz4' AS "SOURCE"."DECODER");
SHOW DECODERS;
DROP DECODER WHERE NAME = 'csv_lz4';
```
`DECODER` is a keyword, so the option key is quoted, as `"SOURCE"` and `INPUT_FORMATTER."TYPE"` are.

## From query to running chain
The decoder chain is not part of the compiled pipelines; it is created together with the source, from the source
descriptor.
1. **`CREATE DECODER`** validates the stage types and options against the registered config schemas and stores the
   named decoder (ordered stage types and options) in the `DecoderCatalog`, which the `SourceCatalog` owns.
2. **`CREATE PHYSICAL SOURCE ... SET ('<name>' AS "SOURCE"."DECODER")`** resolves the name in the `DecoderCatalog` and
   copies the stage list into the physical source's `SourceDescriptor` (`getDecoders()`). Later changes to the named
   decoder do not affect existing sources.
3. **Query planning and placement** treat the descriptor as opaque: the logical plan's `SourceDescriptorLogicalOperator`
   carries it, and it is serialised with the plan to the worker. No optimizer rule or placement step changes.
4. **Query compilation** lowers the source to a `SourceDescriptorPhysicalOperator` and attaches the input formatter to the
   scan of the first pipeline (`PipeliningPhase`), as without a decoder. The compiled plan keeps the source
   descriptors.
5. **Instantiation** of the executable query plan (`nes-runtime/src/ExecutableQueryPlan.cpp`) calls
   `SourceProvider::lower` for every source. It creates the source through the `SourceRegistry` and the chain through
   `DecoderChain::createDecoder(descriptor.getDecoders())`: for every stage, in order, the `DecoderStageRegistry` creates a
   new instance with its instantiated config. Without stages, no chain is created (G5). The chain is moved into the
   `SourceHandle` and from there into the `SourceThread`.
6. **Start.** When the query starts, `RunningSource::create` starts the source thread with the `emit` function of the
   query engine; the thread runs `dataSourceThreadRoutine` with the chain as described in
   [Chain execution](#chain-execution).

Every source instance of every query gets fresh stage instances, so stage state is never shared between sources or
queries.

## Testing
**Unit tests** per plugin feed encoded data in arbitrary chunkings (single bytes, buffer-sized, unaligned) and compare
the output with the original data (round trip: data = decode(encode(data)), with the codec library as encoder). They
cover concatenated frames, corrupt input, and an output buffer that is full exactly at the end of a frame.

**ConnTest** (`nes-conn-test`) runs a source connector in isolation against a real external system (e.g. a TCP server
in Docker) and compares the bytes the source emits with rows generated from the schema. It is extended as follows:
1. The loader encodes the data before sending it (e.g. Zstd-compressed JSON lines); the physical source in the test
   configuration declares the matching decoder.
2. An oracle, a reference implementation of the codec independent of the plugin (e.g. the `zstd` or `lz4` library or
   command-line tool), decodes the bytes that were sent. Its output must equal the generated rows, which validates the
   test input, and the output of the source must equal the output of the oracle byte by byte.
3. Decryption stages are tested the same way, with the reference implementation of the cipher (e.g. OpenSSL) as oracle.

Assessment of the current ConnTest code: the extension is feasible with moderate effort. The harness creates the
`SourceHandle` without a decoder and does not register `CREATE DECODER` statements in its catalog, so both must be added.
ConnTest currently compares tuple-wise, so the byte-wise comparison with the oracle is a new comparison mode. Scenarios
that interrupt the connection (connection loss, crash recovery) cut an encoded stream inside a frame; their expected
outcome depends on the end-of-stream semantics (NG6).

**Micro benchmarks** measure the decoding throughput of each plugin and of a chain in isolation, without a source
(G7).

# Alternatives

## A1 Decoding in the input formatter *(adopted)*
Decode before tuple detection inside `nes-input-formatters`. Rejected: buffers arrive out of order, so the
formatter either forgoes in-order codecs (violates G2) or reorders the buffers and waits for the next sequence number,
which adds complexity and serialises the otherwise parallel formatting.

## A2 Internal byte stream between stages
Each stage writes into memory it allocates itself (e.g. a growing `std::vector<std::byte>`), and the next stage reads
from it; only the last stage writes tuple buffers.
- Advantage: a frame that spans several input buffers is contiguous in memory, which simplifies stage code.
- Disadvantage: this memory does not come from the buffer pool, so neither the pool size nor the inflight limit bounds
  it (G4), and its size is not visible to monitoring.
- Disadvantage: every hand-off copies the bytes once more.
- Disadvantage: the last stage needs a different output path than the others.

## A3 Collect-then-emit
`decode` returns a list of all output buffers produced from one input buffer; only when the list is complete is it
passed on (to the next stage, and finally to the source's `emit`).
- Advantage: simplest stage code.
- Disadvantage: the list for one input buffer can contain thousands of buffers (P5). They are all taken from the pool
  before the first one reaches the source's `emit`, so the inflight limit cannot stop the decoder in between, and the
  pool can run empty for all queries of the worker.
- Disadvantage: the first decoded buffer reaches the query only after the whole input buffer is decoded.
Rejected because of G4.

## A4 Single exchange callback (Leo Raasch's `emitAndProvide`)
The decoder receives one empty buffer per input and obtains the next only by handing over a filled one, through a single
callback that emits to the input formatter; a status flag tells the callback whether another buffer is needed. The
proposed solution adopts this design with two changes: the callback passes buffers on to the next stage instead of the
input formatter, and the status flag is replaced by two operations (`emitAndGetNext`, `emitLast`).

## A5 Separate buffer acquisition
The context offers `getBuffer` in addition to emitting, and a stage obtains its first output buffer of a call on demand.
- Advantage: a call that produces no output obtains no buffer.
- Disadvantage: the stage obtains buffers from the pool itself, and the bound of one output buffer per stage can only be
  checked at runtime: a plugin that requests a second buffer before passing on the first compiles and fails when it
  runs.
Rejected in favour of keeping buffer acquisition in the chain.

## A6 Decoupled stages with a buffer budget
Each stage takes as many buffers as it needs and emits when it is ready, into a bounded queue in front of the next
stage. Each stage has a budget of pool buffers; a stage that has exhausted its budget blocks until the
next stage has consumed buffers.
- Advantage: stages can work ahead of each other within their budget, which smooths bursts (e.g. a large frame).
- Advantage: stages can run as separate tasks on different threads.
- Disadvantage: memory is bounded by the sum of per-stage budgets instead of by the number of stages, and the budgets
  are additional configuration parameters that must fit the pool size.
- Disadvantage: on a single source thread, it behaves like the proposed depth-first execution except for the work-ahead.
Not chosen for the source-thread implementation. Since a stage only exchanges buffers through its context, the interface permits
this execution model later without changes to plugins.

# Proof of Concept
Branch [`oc/decoder-prototype`](https://github.com/nebulastream/nebulastream/tree/oc/decoder-prototype) contains a
working prototype:
- `nes-decoder`: the stage interface, the chain, the stage registries, and `CREATE DECODER`, `SHOW DECODERS` and
  `DROP DECODER`, including systest support;
- `nes-plugins/Decoders/LZ4Decoder` and `nes-plugins/Decoders/ZstdDecoder`, with unit tests for arbitrary input
  chunking, concatenated frames, truncated and corrupt input, and chains of both codecs (Zstd then LZ4, LZ4 then Zstd);
- a systest that reads an LZ4-compressed CSV file with the same result as the uncompressed file.

The prototype differs from this document in one respect: its stages use a collect-then-emit interface (A3) instead of
the exchange interface proposed here.

# Summary
Decoders run in the source thread, where buffers are in order (P2, G2), as in Leo Raasch's design. This document
generalises the single decoder to an ordered chain of plugin stages with no fixed kind or position (P4, G3, G8),
exchanging tuple buffers from the source's pool. Emission is eager and depth-first through the source's `emit`, which
bounds the memory of a chain independently of the compression ratio and keeps the inflight limit and stop effective
(P5, G4). Stages return a dedicated error for input they cannot decode. Plugins are tested against a reference
implementation of their codec in ConnTest (G6). Chains are declared with `CREATE DECODER` and validated before a query
runs (G8); LZ4, Zstd and AES-CTR are the first plugins (G7). A1 fails on
ordering, A2 on resource accounting, A3 on bounded memory; A4 is adopted with minor changes, A5 gives stages access to the pool, and A6 remains possible
on the same interface.

Answers to the design questions of #2186:
| Question | Answer |
|---|---|
| Composition | Ordered chain of arbitrary stages; tuple buffers between stages, from the source's pool, owned by whoever holds the reference |
| n:m buffers | Any number of emits per `decode`; codec state carries incomplete frames; `flush` at end of stream; undecodable input returns an error |
| Resources, backpressure | Depth-first emission through the source's `emit` and its inflight limit; a blocked `emit` blocks the source thread, which stops reading and lets the external system's flow control take effect; one output buffer per stage, so at most *n + 1* buffers per chain; backpressure listener unchanged; buffers obtained by the chain with stop-aware waits |
| Registry | `DecoderStageRegistry` and schema registry, plugins under `nes-plugins/Decoders/` |
| Configuration | Named decoders (`CREATE DECODER`), resolved into the source descriptor |
| Metadata | Intermediate buffers carry only their number of valid bytes; the source's `emit` assigns the existing metadata; no additional metadata for now |
| Async IO | Open, see below |

# Open Questions
1. **Stages that retain buffers:** how to support stages whose units span input buffers and are not buffered by a
   codec library, and what bounds the size of such a unit?
2. **Async IO, scope:** which part of the integration with the async IO runtime (#2154) does #2186 refer to: decoding for
   sources hosted on that runtime, running decoder stages as tasks of that runtime, or only the interface not
   precluding either?
3. **Async IO, language boundary:** #2154 proposes a Tokio-based runtime with connectors written in Rust. Does the decoder
   chain run inside that runtime (C++ stages called from Rust, or stages in Rust), after it in C++, or only for
   thread-per-source sources?
4. **Async IO, execution model:** should each decoder stage become a separate unit of work, so that a high-throughput
   source can use several threads and many low-throughput sources can share one? How is a stage suspended and resumed
   when obtaining or passing on a buffer cannot block, and does its state then have to be managed by the framework?
