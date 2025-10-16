# The Problem
Currently, NebulaStream is able to receive and process data in `formats` such as CSV, JSON and Native, more formats like HL7 v2 are likely going to be added in the future. The component responsible for detecting tuples in the raw buffers filled with data in these formats is the `nes-input-formatter`. 

Similar to `data formats` are `codecs`. Data of a certain format can be encoded according to the rules of a certain codec. Exemplary for this are general purpose `compression codecs` like Facebook's "Zstandart" (Zstd)[1]
or Google's "Snappy"[2], which are able to compress and decompress data byte per byte, independent of the data type or format.

If data is encoded, the input-formatter cannot rely on the characteristic tuple and field `delimiters` of the format to extract tuples from the raw buffers. Therefore, we need a new `decoder component` that decodes incoming tuple buffers before they are formatted (**P1**).

This problem is also addressed in the readme file of the nes-input-formatter[3], section "Codecs". There, it is stated that decoders should either belong to the `nes-sources` or the `nes-input-formatters` component. The latter currently receives and formats buffers in an asynchronous fashion. However, many compression codecs such as `LZ4`[4] need the data to arrive `in-order`, since the encoded data usually references reoccurring byte patterns via pointer to previous raw bytes. Therefore, decoding in the input-formatter could cause a bottleneck (**P2**).

Furthermore, data formats like `Apache Parquet`[5] use light weight and general purpose compression codecs to encode the data of column pages. If we want to include Parquet as a format, we need decoders for the possibly used encodings (**P3**).

# Goals

- describe goals/requirements that the proposed solution must fulfill
- enumerate goals as G1, G2, ...
- describe how the goals G1, G2, ... address the problems P1, P2, ...

# Non-Goals
- list what is out of scope and why
- enumerate non-goals as NG1, NG2, ...

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
