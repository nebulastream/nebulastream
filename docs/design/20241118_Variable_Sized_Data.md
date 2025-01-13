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

# Important Notes:
The following contains all notes for a possible knowledge transfer:
## General Onboarding Topic:
- Title: Variable Size Data Type Support in NebulaStream
- Prerequisites: The general architecture of data types will be ready by then
- Challenge: Requires and end-to-end overview of the entire code base from API down to the compiled code
- Tasks:
	- Familiarize with current data type support especially the rudimentary composite and string type support
	- Integrate support end-to-end, from query API to query compiled operators and functions
	- (Optional) Investigate optimized physical representation such as „German-style strings“ or compressed formats
- Deliverable: Set of running queries, preferably in the form of system level tests
- Scope: We want things working correctly first...
## Important thoughts:
- Most important questions to answer:
	- [TupleBuffer] How to read/write from/to the buffer w.r.t concrete format?
		- PS: Create "own" chaining solution, based on pointers and lengths to calculate offsets in var-sized-data memory
		- DuckDB uses a string heap (see [here](https://github.com/duckdb/duckdb/blob/19864453f7d0ed095256d848b46e7b8630989bac/src/include/duckdb/common/types/string_heap.hpp#L4))
  - [TupleBuffer] How to handle overflows in operations e.g. when appending strings (concatenate) ?
		- PS1: re-alloc strategies / overflow buffers as reserves
		- PS2: Linked list of string heaps / buffers
		- DuckDB uses "overflow buffers" to prevent frequent re-allocations and fragmentation within main storage buffers
		- ClickHouse uses a secondary buffer strategy which keeps large or "overflowed" data segments in a separate area
	- [Log vs. Phy] How to design generic 'varSizedDataType' and extend with concrete classes (TEXT, BLOB, List, etc.) with minimal impact on current codebase?
		- Each format has its own class (and inherits from generic one?)
	- [TupleBuffer] How do we manage life cycle of the pointers that are stored in the var-sized fields? How to delete/free them all and when?
		- Assumptions: buffermanager is aware and manages it => how does he know that no pointer is still used? => shared pointer / reference counters?
		- Assumptions: Each tuple buffer owns its own list of string heaps (1:N)
		- BUFFER RECYCLING w.r.t to bufferManager: see destructors in tupleBufferImpl.cpp -> TupleBuffer::release() logic via recycleCallback()
  - Discussion on memory layouts -> Big question: how do we get rid of the add. heap buffers required for var sized data? https://github.com/nebulastream/nebulastream-public/discussions/528
## Meeting with Adrian (22.10):
- Most IMPORTANT: koennen wir mir TEXT alles abdecken? also String, BLOB, LIST of same datatypes
- Check varSizedData.cpp, loadvalue() to understand nautilus layer

Step 1: write test cases for selection, like variableSizeDataTypesTest mit schema, selection operator (Equals)
	-- Filter-Test: schauen ob man mit typ den filter operator erweitern kann, 
    expression, "Selection test" function in logischen nes-execution (Function.hpp)
    -- dann aendert man schema mit vlt varBinary type (physicalType) und dann funktioniert die selection noch, check??!!
    -- see VariableSizedDataTest.cpp to start with UnitTest
    -- Use testTupleBuffer if necessary

- PR that changes TEXT to varDataSizeType: https://github.com/nebulastream/nebulastream-public/pull/378/files 
    -- check changing files
- check tupleBuffer[tupleCount].writeVarSized(schemaFieldIndex, inputString, bufferProvider);
## Meeting with Nils (28.10):
- Write a system level test using TEXT type to see end2end functionality
- Think about how to store stuff, the formats, and how to read

3 major design decisions:
(1) write to tuple buffer (using String, BLOB, etc. format)
(2) read from tuple buffer using concrete format
(3) what happens on tuple buffer side when e.g. 2 strings concatenate and outbuffer does not have enough memory for the result?

Check related PR: https://github.com/nebulastream/nebulastream-public/commit/ca303d76bacb16d20b10622eaf17af6b2528d4be
## General aspects:
- What do we mean with variable sized data types?
    -- Data that is stores as strings, blobs, or variable-length arrays/list

### VARCHR(N) in general:

- The actual storage used for the data depends on the length of the string stored in the field, not on the maximum size defined by N
- E.g., if VARCHAR(100) is defined and you store a string of length 10, the system will only use space for 10 characters plus a small amount of overhead for keeping track of the length, rather than the full 100 characters.

### German Strings:
- https://cedardb.com/blog/german_strings/
- String = 128 bit struct (can be passed via 2 registers instead of putting them on the stack)
    -- Short string: len(32 bit) + content(96 bit) 'in place'
    -- Long string: len(32 bit) + prefix(32 bit) + ptr(62 bit) ptr--->data
        -- len with 32 bits limits string size to 4GiB -> make tradeoff?
        -- prefix saves pointer dereference
- Downside: Appanding data to a string is expensive, we need to allocate a new buffer and payload must be copied
- Questions to ask:
    -- What is the lifetime of my string?
    -- Can I get away with a transient string, or do I have to copy it?
    -- Will my strings be updated often? 
    -- Am I okay with immutable strings?
## How is Flink doing it:
 
VARCHAR:
- VARCHAR(n) impl.: https://github.com/apache/flink/blob/master/flink-table/flink-table-common/src/main/java/org/apache/flink/table/types/logical/VarCharType.java
- VARCHAR(n) where n is the max. number of code points
- n is a value between 1 and INTEGER.MAX_VALUE
- if no length is specified, n = 1
- primarly used to impose storage constraints

STRING:
- STRING = VARCHAR(2147483647)
- STRING is an unbounded data type, meaning the length is not restricted, i.e. no length constraint required
- VARCHAR(n) allows for setting a specific length limit

- DatatypeFactory: https://github.com/apache/flink/blob/master/flink-table/flink-table-common/src/main/java/org/apache/flink/table/catalog/DataTypeFactory.java

- String processing in Flink: https://chatgpt.com/c/67162061-adfc-8002-9390-dc77da1b4e09
- StringValue: https://github.com/apache/flink/blob/master/flink-core/src/main/java/org/apache/flink/types/StringValue.java (includes read and write, string operations, ...)
- StringSerializer: https://github.com/apache/flink/blob/master/flink-core/src/main/java/org/apache/flink/api/common/typeutils/base/StringSerializer.java
## NebulaStream internals:

### Data types in NebulaStream:
- logical and physical type, e.g. TextPhysicalType represents log. textType and FixedChar
- DataTypeFactory creates the data type based on the BasicType and returns shared pointer [DataTypeFactory::createType()] to buffer of type data type
- Every data type inherits from class DataType()

### Schema in NebulaStream:
- See SchemaTest.cpp for examples
- Schema adds a field and calls internal data type factory method, e.g. Schema::create()->addField("field1", UINT8) and return pointer Schema pointer ("piece of memoryLayout")
- memoryLayout is either row or columnar, see class MemoryLayoutType

### (De)Serialization using Protobuf:
- Queryplan, data types get serialized and desirialzed
- Data type (de)serialization in DataTypeSerializationUtil.cpp
- Need to define both directions when adding a new type
- See SerializableDataType for types definition -> generated C++ file in SerializableDataType.pb.h
- deserializeDecomposedQueryPlan() deserializes not only query plan, also data types etc.
- Data type gets constructed when desirialized

### Buffer management using TupleBuffer:
- TestTupleBuffer is a warpper around TupleBuffer that contains more functionality, but less performant due to virtual function calls
- BufferManager gets created with specified bufferSize and number of buffers, e.g. Memory::BufferManager::create(4096, 10);
- Read and write var sized data into buffer, see MemoryLayout.cpp in writeVarSizedData() / readVarSizedData()
- loadValue() in TupleBufferMemoryProvider: loads an VarVal of type from a fieldReference
    -- checks if basicType or VariableSizedData and return an VarVal
    -- typ3.5e == isBasicType: Nautilus::VarVal::readVarValFromMemory(fieldReference, type);
    -- type == isVarSizedDataType: chaining buffer logic, calc. chilIndex, then get ptr to data via loadAssociatedTextValue(), then return Nautilus::VariableSizedData(textPtr);
- -- readVarSized() to return childbuffer on index, to know where to start (TestTupleBuffer.cpp)
        -- calls 'return readVarSizedData()' that actually returns the string vlaue (MemoryLayout.cpp)

### VarVal class:
- In nes-nautilus VarVal.cpp/.hpp
- Base class of all data types in the nautilus backend, i.e. all data types, fixed and varibale, are derived from here
- Sits on top of its val<> data type
- Defining operations on VarVal: uses std::variant and std::visit to automatically call the already existing operations on the underlying nautilus::val<> data types
- VarSizedDataType defines custom operations within the class itself

### VariableSizedData class:
- In nes-nautilus VariableSizedData.cpp/.hpp
- First 4 bytes are assumed to be the length of the actual data
- No standalone class, should be used via the VarVal class
- or the VarSizedDataType, we define custom operations in the class itself (see VarVal.hpp)

### General flow of system tests (and query processing) via example via concrete test ST_Filter.test:
- SystemTest::TestBody() main executed function
    (1) Query plan Serialisation "SerializableDecomposedQueryPlan" using protobuf => gets parsed from input-filestream
    (2) Query plan gets registered via registerQueryPlan() which registers query on singleNodeWorker and executes compileQuery()
    (3) NautilusQueryCompiler::compileQuery() executes different phases (logical, physical, etc.) and returns result at the end
        (3.1) Logical query plan gets created from decompesed query plan of a certain request, returns DecomposedQueryPlanPtr
        (3.2) Physical query plan gets created from logical plan via lowerLogicalToPhysicalOperatorsPhase() which iterates through
                logical operator nodes (e.g. source, sink, filter, etc.) and replaces each with its correspondiong physical one
        (3.3) Physical query plan gets transformed into a plan of pipelines, see pipeliningPhase->apply(physicalQueryPlan)
            -> a new pipeline for each sink operator
        (3.4) Another "simple" phase to add scan and emit operator to pipelines plan via addScanAndEmitPhase->apply(pipelinedQueryPlan)
        (3.5) Pipeline plan of physical operators gets lowered into a pipeline plan of nautilus operators (LowerPhysicalToNautilusOperators phase)
            -> Lowering of each individual operator is defined by the nautilus provider
        (3.6) Compilation phase (NautilusCompilationPhase): generates executable machine code for pipelines of nautilus operators
            -> pipeline operators get replaced with executable operators
        (3.7) At the end, "LowerToExecutableQueryPlanPhase" creates and executable query plan for each node engine

        ==> A QueryCompilationResult is returned for the node engine
    (4) Operators execute() functions are executed on the actual data ...
## Clickhouse vs. DuckDB - How do they handle var. sized data internally:

### DuckDB:
- Separate Storage for Fixed and Variable-Length Data:
    -- Var sized data is handled using (1) offsets pointing to the actual data and (2) overflow pages that store the actual data 
        in a separate block so that it can grow and shrink here
- For Strings, they use Compression->Dictionary Encoding to improve cache efficiency and reduce memroy usage
- Memory Management for Variable-Length Data:
    -- Var sized data is processed using a combination of memory blocks and overflow handling
    -- Main Memory Blocks: Variable-length data is stored in main memory blocks until it reaches a certain size limit
    -- Overflow Blocks: When the main memory block overflows, DuckDB spills excess data into overflow blocks. 
        These blocks are linked back to the original column, allowing the system to retrieve the data efficiently when needed.
- For variable-sized data, DuckDB can store segments in memory and link them to overflow pages. This segmentation improves memory locality and query performance.
- Query Execution:
    - By using offsets and overflow pages, DuckDB can efficiently access only the relevant parts of the variable-length data, avoiding unnecessary overhead.
- Variable-length values, such as strings, represent a native array of pointers in a separate string heap (src/include/duckdb/common/types/string_heap.hpp)

### Clickhouse:
Upper Layers:
- Generally uses a map to keep track of all data types => using DataTypesDictionary = std::unordered_map<String, Value>;
- DataTypeFactory creates data types via factory.registerDataType("String", create);
    -- simply adds the data type name to the map "DataTypesDictionary"
- Data type String: https://github.com/ClickHouse/ClickHouse/blob/master/src/DataTypes/DataTypeString.cpp
    -- In registerDataTypeString():
        -- String gets registered via factory.registerDataType("String", create)
        -- Subsequently, different synonms are added via factory.registerAlias(...) for entension/compatibilty, 
            e.g. factory.registerAlias("NVARCHAR", "String", DataTypeFactory::Case::Insensitive);
        -- registerDataTypeString() gets called in DataTypeFactory::DataTypeFactory() to register all these variants, i.e. they are mapped to String type

Memory Management:
- Clickhouse allocates memory in large chunks (pools) that are then reused (instead of frequent dynamic memory allocations)
    -- Strategy that preallocates a buffer large enough to handle expected size of strings (growing it only if necessary)
- Mix of memory pooling, buffer management, efficient allocation startegies and memory resue techniques

Memory Formats (in src->Processors->Formats):
- They use a IRowInputFormat reader and IRowOutputFormat writer for
## Add. questions to answer for design doc:
- How does the String representation look like? Something like German Strings?
- How do we handle (De)Serialization efficiently? w.r.t Protouf. e.g. Flink serializes strings as UTF-8 encoded byte arrays
- Memory Management for Strings?
- Design an abstract datatype 'VarSizedDT' that other custom data types unnecessary
    -- each instantiation should define how its memory layout looks like...
    -- How do we know what the concrete VarSizedDT is? We need to add this information from parser into the concrete Datatype
## Keep in mind:
- Aljoscha: Concept 'ContainsString' in NESType.hpp (soon in TestTupleBuffer.hpp) should be generalized for VariableSizedDataType
    -- https://github.com/nebulastream/nebulastream-public/pull/378/files/d47e9f497a009be092a9b80c244f45094e5c0a2b#r1812625598