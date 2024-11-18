# The Problem
- describe the current state of the system
- explain why the system's current state requires a change
- specify the concrete problems and enumerate them as P1, P2, ...

Supporting Variable-Sized Data in a query engine is essential to handle diverse data types and dynamic storage needs,
especially for real-world applications like text, multimedia, and logs. In streaming databases, this capability is even 
more critical as data arrives in unpredictable sizes and formats, such as IoT sensor readings, video streams, or event logs.
Efficient handling of variable-sized data ensures that the database can process, store, and query streaming inputs in real-time
without excessive overhead or data loss, maintaining the system’s performance and responsiveness.

At this time, the NebulaStream query engine does not support an **end-to-end data processing on Variable-Sized Data Types (P1)** like
TEXT (strings or VARCHAR), BLOB ("raw" binary data) or LISTS/ARRAYS of elements with one single type (e.g. array(int)).
End-to-end means that a user or system-test is able to submit queries that involve e.g. String operations on an input stream (SOURCE)
and gets a result on a SINK.

The original NebulaStream repository (https://github.com/nebulastream/nebulastream) currently has an implementation of a TEXT data type which
is used store arbitrary sequences of characters. However, this data type is restricted w.r.t to **multiple physical layouts (P2)**
,i.e. how the data is represented in-memory, which blocks further optimizations such as "german strings" (https://cedardb.com/blog/german_strings/), or compression formats such as dictionary encoding,
LZ4, Snappy, etc.

As soon as we have a clear implementation for Variable-Sized Data Types, we also need to **support different operations and
sources types (P3)** on these data types.


# Goals
- describe goals/requirements that the proposed solution must fulfill
- enumerate goals as G1, G2, ...
- describe how the goals G1, G2, ... address the problems P1, P2, ...

This design doc mainly focuses on an end-to-end data processing capability for TEXTUAL DATA / STRINGS but in a way that the internal
data type representation can be extended to other types of variable-sized data, e.g. BLOBs or ARRAYs (G1 which addresses P1). Again,
end-to-end means that we can execute simple queries, e.g. (SOURCE)->(FILTER)->(SINK), from the system-test perspective.
Generally, the solution will touch multiple layers, like our parser, execution engine and the nautilus query compiler, with the goal 
to change minimal core components' code.

End-to-end also means that query operators (e.g. projection, map, filter, etc.) in NebulaStream can process strings with string
operations such as subString() or concatenate() (G2 which addresses P3).

Overall, the in-memory representation in the TupleBuffer should be designed in a way to leave space for further optimizations such
as compression for future/research work (G3 which addresses P2). This involves designing read() & write() functionality 
w.r.t the TupleBuffer.

# Non-Goals
- list what is out of scope and why
- enumerate non-goals as NG1, NG2, ...

As we already mentioned, this design doc only focuses on one specific variable-sized data type: TEXT/STRINGS.
Out of scope is the end-to-end implementation for BLOBs, ARRAYs, and other related var-sized types (NG1).

Additionally, optimizations such as string compression format are out of scope for now (NG2) and more like a nice-to-have thing.

TODO: what string operations are out-of-scope?

# (Optional) Solution Background
- describe relevant prior work, e.g., issues, PRs, other projects

TODO: Mention how duckDB is doing it via string heap, overflows, etc.

TODO: Mention how FLink is doing it via stringValue, stringSerializer, etc.

# Our Proposed Solution
- start with a high-level overview of the solution
- explain the interface and interaction with other system components
- explain a potential implementation
- explain how the goals G1, G2, ... are addressed
- use diagrams if reasonable

Important assumptions:
- Strings are immutable, i.e. that once a string value is created in the database, it cannot be altered directly. 
Instead, any operation that modifies a string (e.g., concatenation, replacement, or trimming) produces a new string value 
without altering the original. This makes our life easier for now w.r.t. memory management.
  - Performance: Immutability ensures data consistency and eliminates the need for complex locking mechanisms during concurrent queries.
  - Compression: Immutability aligns well with efficient compression and query execution.
  - Safety: Immutable strings prevent unintended side effects, making operations safer in concurrent environments.
- Buffer manager handles memory allocation and is responsible for handling life cycle of pointers to var-sized data.
- String are encoded using UTF8. Most databases, including DuckDB, PostgreSQL, and MySQL, support UTF-8 as the preferred encoding for text.
  - Support a vast range of characters, enabling multi-language data storage.
  - Use space efficiently, especially for predominantly ASCII data.
  - Align well with industry standards for data exchange and APIs (e.g., JSON, HTML). 

# Proof of Concept
- demonstrate that the solution should work
- can be done after the first draft

# Alternatives
- discuss alternative approaches A1, A2, ..., including their advantages and disadvantages

# (Optional) Open Questions
- list relevant questions that cannot or need not be answered before merging
- create issues if needed

# (Optional) Sources and Further Reading
- list links to resources that cover the topic

# (Optional) Appendix
- provide here nonessential information that could disturb the reading flow, e.g., implementation details
