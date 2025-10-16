# The Problem
Currently, NebulaStream is able to receive and process data in `formats` such as CSV, JSON and Native, more formats like HL7 v2 are likely going to be added in the future. The component responsible for detecting tuples in the raw buffers filled with data in these formats is the `nes-input-formatter`. 

Similar to `data formats` are `codecs`. Data of a certain format can be encoded according to the rules of a certain codec. Exemplary for this are general purpose `compression codecs` like Facebook's "Zstandart" (Zstd)[1]or Google's "Snappy"[2], which are able to compress and decompress data byte per byte, independent of the data type or format.

(**P1**): If data is encoded, the input-formatter cannot rely on the characteristic `tuple and field delimiters` of the format to extract tuples from the raw buffers. Therefore, we need a new `decoder component` that decodes incoming tuple buffers before they are formatted.

(**P2**) This problem is also addressed in the readme file of the nes-input-formatter[3], section "Codecs". There, it is stated that decoders should either belong to the `nes-sources` or the `nes-input-formatters` component. The latter currently receives and formats buffers in an asynchronous and unordered fashion. 
However, many compression codecs such as `LZ4`[4] need the data to arrive `in-order`, since the encoded data usually references reoccurring byte patterns via pointer to previous raw bytes. 
Therefore, decoding in the input-formatter could cause a bottleneck.

(**P3**) Furthermore, data formats like `Apache Parquet`[5] use light weight and general purpose compression codecs to encode the data of column pages. If we want to include Parquet as a format, we need decoders for the possibly used encodings. 
Supporting formats like Parquet or just compression codecs in general could help us achieve higher throughput for TCP source data, when dealing with a `fixed connection bandwidth`, which otherwise could cause a bottleneck at the source.

# Goals
To solve the described problems, a nes-codec component with the following properties should be added:

**G1** Decoders for codecs. 
For each codec, that we decide to support, a decoding function should be provided.
It should be able to decode tuple buffers incrementally since we operate in a data stream context and do not receive one encoded block at a time.
(**P1, P3**)

**G2** In-order decoding. 
The decoding functions assume that the tuple buffers arrive in-order.
Thus, tuple buffers should be decoded in a component, where the system still obtains them in-order.
Except for the decoding itself, the integration of the decoder should ideally not cause any additional work like ordering of the buffers or waiting for missing buffers.
(**P2**)

**G3** Easily extensible.
There is a broad variety of codecs, and we cannot offer decoders for each one of them.
Adding a decoder should only require the addition of the decoding function, without changing any other components of the system.
This way, adding support for a new codec or format with codecs is straightforward. 
(**P3**)

# Non-Goals
**NG1** Encoders for codecs.
The primary focus of this design document is the decoding of incoming data before it is formatted to tuples. 
On demand encoding query results before emitting them to the sink affects different components than the decoders.
The benefits and challenges of this practice should be elaborated upon in a separate design document.

**NG2** Decoding and encoding during query execution.
Sending compressed data between pipelines and only decompressing the data if an operation in the pipeline demands it could increase the throughput. 
However, such a feature would require changes of many components such as the query plan optimizer, operators and also the implementation of **NG1**.  

# (Optional) Solution Background
- describe relevant prior work, e.g., issues, PRs, other projects

# Our Proposed Solution
- start with a high-level overview of the solution
- explain the interface and interaction with other system components
- explain a potential implementation
- explain how the goals G1, G2, ... are addressed
- use diagrams if reasonable

# Proof of Concept
- demonstrate that the solution should work
- can be done after the first draft

# Alternatives
- (arguably the most important section of the DD, if there are no good alternatives, we can simply implement the sole solution and write documentation)
- discuss alternative approaches A1, A2, ..., including their advantages and disadvantages


# Summary
- this section closes the Problems/Goals bracket opened at the beginning of the design document
- briefly recap how the proposed solution addresses all [problems](#the-problem) and achieves all [goals](#goals) and if it does not, why that is ok
- briefly state why the proposed solution is the best [alternative](#alternatives)

# (Optional) Open Questions
- list relevant questions that cannot or need not be answered before merging
- create issues if needed

# (Optional) Sources and Further Reading
[1] https://github.com/facebook/zstd

[2] https://github.com/google/snappy

[3] https://github.com/nebulastream/nebulastream/blob/main/nes-input-formatters/README.md

[4] https://github.com/lz4/lz4

[5] https://parquet.apache.org/docs/file-format/

# (Optional) Appendix
- provide here nonessential information that could disturb the reading flow, e.g., implementation details
