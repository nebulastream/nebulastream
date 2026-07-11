# NebulaStream Extension Requirements

> **For agents (`/generate-extension` skill)**: This file is your authoritative code template.
> Match the `type` field from the description file to the numbered section below (§1 function, §2 operator, §3 source, §4 sink).
> Substitute all placeholder names, expand `ConfigParameters` from the description, and insert `// TODO:` comments from `## Behavior`/`## Examples`.
> Always include `Reflector<T>` / `Unreflector<T>` structs for functions and operators — they are required for plan serialization.
> See `extension-description-format.md` in this directory for the description file schema.
> See `docs/guide/` for human-readable walkthroughs.
> After generating C++ files, also generate a systest using the templates in §5.

This document is a reference for adding new **functions**, **operators**, **sources**, and **sinks** to NebulaStream.
Each section follows the same structure: concept → files to create → interface → CMake registration → skeleton code
drawn from a real working example.

All four extension points share the same underlying mechanism:
a CMake `add_plugin(Name RegistryType file.cpp)` call causes the build system to auto-generate a `.inc` file that
wires the new type into the appropriate registry. No hand-editing of registry files is needed.

---

## 1. New Function

### What it is

A **function** is an expression node in a query (e.g. `ABS(x)`, `ADD(a, b)`, `CAST(x AS INT)`).
Functions exist at two levels that must both be implemented:

- **Logical function** — high-level, type-checked representation used during query planning and optimization.
- **Physical function** — low-level, JIT-compiled representation used during execution (Nautilus IR).

Both are resolved by name through their respective registries. The SQL parser looks up function names
case-insensitively, so `abs`, `ABS`, and `Abs` all resolve to the same entry.

### Files to create

| File | Purpose |
|------|---------|
| `nes-logical-operators/include/Functions/<Category>/MyFuncLogicalFunction.hpp` | Logical function header |
| `nes-logical-operators/src/Functions/<Category>/MyFuncLogicalFunction.cpp` | Logical function implementation + registry entry |
| `nes-physical-operators/include/Functions/<Category>/MyFuncPhysicalFunction.hpp` | Physical function header |
| `nes-physical-operators/src/Functions/<Category>/MyFuncPhysicalFunction.cpp` | Physical function implementation + registry entry |

### Interface to implement

**Logical** (`LogicalFunctionConcept` — `nes-logical-operators/include/Functions/LogicalFunction.hpp`):

```cpp
class MyFuncLogicalFunction final {
public:
    static constexpr std::string_view NAME = "MyFunc";   // used as registry key

    explicit MyFuncLogicalFunction(LogicalFunction child /*, more args */);

    [[nodiscard]] std::string_view getType() const;       // return NAME
    [[nodiscard]] DataType getDataType() const;
    [[nodiscard]] std::vector<LogicalFunction> getChildren() const;
    [[nodiscard]] MyFuncLogicalFunction withChildren(const std::vector<LogicalFunction>& children) const;

    // Validates input types and sets dataType; throw CannotInferStamp on type mismatch
    [[nodiscard]] LogicalFunction withInferredDataType(const Schema<Field, Unordered>& schema) const;

    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const;
    [[nodiscard]] bool operator==(const MyFuncLogicalFunction& rhs) const;

private:
    DataType dataType;
    LogicalFunction child;
    friend Reflector<MyFuncLogicalFunction>;
};
static_assert(LogicalFunctionConcept<MyFuncLogicalFunction>);
```

**Physical** (`PhysicalFunctionConcept` — `nes-physical-operators/include/Functions/PhysicalFunction.hpp`):

```cpp
class MyFuncPhysicalFunction final {
public:
    explicit MyFuncPhysicalFunction(PhysicalFunction childFunction, DataType inputType, DataType outputType);
    // Core execution — called once per record in the JIT-compiled pipeline
    [[nodiscard]] VarVal execute(const Record& record, ArenaRef& arena) const;
private:
    PhysicalFunction childFunction;
    DataType inputType;
    DataType outputType;
};
static_assert(PhysicalFunctionConcept<MyFuncPhysicalFunction>);
```

### CMake registration

```cmake
# nes-logical-operators/src/Functions/<Category>/CMakeLists.txt
add_plugin(MyFunc LogicalFunction MyFuncLogicalFunction.cpp)
add_unreflection_plugin(LogicalFunction MyFunc)   # required for plan serialization

# nes-physical-operators/src/Functions/<Category>/CMakeLists.txt
add_plugin(MyFunc PhysicalFunction MyFuncPhysicalFunction.cpp)
```

### Skeleton (based on `Abs`)

**`MyFuncLogicalFunction.cpp`** (key parts):
```cpp
#include <Functions/<Category>/MyFuncLogicalFunction.hpp>
#include <LogicalFunctionRegistry.hpp>
#include <ErrorHandling.hpp>

namespace NES {

MyFuncLogicalFunction::MyFuncLogicalFunction(LogicalFunction child) : child(std::move(child)) {}

DataType MyFuncLogicalFunction::getDataType() const { return dataType; }

LogicalFunction MyFuncLogicalFunction::withInferredDataType(const Schema<Field, Unordered>& schema) const {
    MyFuncLogicalFunction copy = *this;
    copy.child = child.withInferredDataType(schema);
    if (!copy.child.getDataType().isNumeric())   // adjust validation as needed
        throw CannotInferStamp("MyFunc requires numeric input, got {}", copy.child);
    copy.dataType = copy.child.getDataType();
    return copy;
}

std::vector<LogicalFunction> MyFuncLogicalFunction::getChildren() const { return {child}; }

MyFuncLogicalFunction MyFuncLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const {
    PRECONDITION(children.size() == 1, "MyFuncLogicalFunction requires exactly one child");
    auto copy = *this;
    copy.child = children[0];
    return copy;
}

std::string_view MyFuncLogicalFunction::getType() const { return NAME; }
bool MyFuncLogicalFunction::operator==(const MyFuncLogicalFunction& rhs) const { return child == rhs.child; }

std::string MyFuncLogicalFunction::explain(ExplainVerbosity verbosity) const {
    if (verbosity == ExplainVerbosity::Debug)
        return fmt::format("MyFuncLogicalFunction({} : {})", child.explain(verbosity), dataType);
    return fmt::format("MYFUNC({})", child.explain(verbosity));
}

// Registry entry — name must match add_plugin() first argument
LogicalFunctionRegistryReturnType LogicalFunctionGeneratedRegistrar::RegisterMyFuncLogicalFunction(
    LogicalFunctionRegistryArguments arguments) {
    if (arguments.children.size() != 1)
        throw CannotDeserialize("MyFuncLogicalFunction requires exactly one child");
    return MyFuncLogicalFunction(arguments.children[0]);
}
} // namespace NES
```

**`MyFuncPhysicalFunction.cpp`** (key parts):
```cpp
#include <Functions/<Category>/MyFuncPhysicalFunction.hpp>
#include <PhysicalFunctionRegistry.hpp>
#include <ErrorHandling.hpp>

namespace NES {

MyFuncPhysicalFunction::MyFuncPhysicalFunction(PhysicalFunction childFunction, DataType inputType, DataType outputType)
    : childFunction(std::move(childFunction)), inputType(std::move(inputType)), outputType(std::move(outputType)) {}

VarVal MyFuncPhysicalFunction::execute(const Record& record, ArenaRef& arena) const {
    auto value = childFunction.execute(record, arena);
    // TODO: apply transformation using Nautilus VarVal ops
    return value.castToType(outputType.type);
}

// Registry entry
PhysicalFunctionRegistryReturnType PhysicalFunctionGeneratedRegistrar::RegisterMyFuncPhysicalFunction(
    PhysicalFunctionRegistryArguments args) {
    PRECONDITION(args.childFunctions.size() == 1, "MyFuncPhysicalFunction requires exactly one child");
    PRECONDITION(args.inputTypes.size() == 1, "MyFuncPhysicalFunction requires exactly one input type");
    return MyFuncPhysicalFunction(args.childFunctions[0], args.inputTypes[0], args.outputType);
}
} // namespace NES
```

---

## 2. New Operator

### What it is

An **operator** is a node in the query plan that transforms a stream (e.g. `Selection`, `Projection`,
`WindowedAggregation`, `Join`). Adding one requires three components:

- **Logical operator** — represents the operator in the logical plan, carries schema inference and traits.
- **Physical operator** — executes record-by-record within a compiled pipeline stage.
- **Lowering rule** — bridges logical→physical during query compilation.

For stateful operators (joins, windowed aggregations) an additional **`OperatorHandler`** is needed to manage
runtime state across records and pipeline invocations.

### Files to create

| File | Purpose |
|------|---------|
| `nes-logical-operators/include/Operators/MyOpLogicalOperator.hpp` | Logical operator header |
| `nes-logical-operators/src/Operators/MyOpLogicalOperator.cpp` | Implementation + registry entry |
| `nes-physical-operators/include/MyOpPhysicalOperator.hpp` | Physical operator header |
| `nes-physical-operators/src/MyOpPhysicalOperator.cpp` | Physical operator implementation |
| `nes-query-compiler/private/LoweringRules/LowerToPhysical/LowerToPhysicalMyOp.hpp` | Lowering rule header |
| `nes-query-compiler/src/LoweringRules/LowerToPhysical/LowerToPhysicalMyOp.cpp` | Lowering rule implementation + registry entry |

### Interface to implement

**Logical** (`LogicalOperatorConcept` — `nes-logical-operators/include/Operators/LogicalOperatorFwd.hpp`):

```cpp
class MyOpLogicalOperator : public ManagedByOperator {
public:
    // Constructors take WeakLogicalOperator self (required by ManagedByOperator)
    explicit MyOpLogicalOperator(WeakLogicalOperator self, /* params */);
    MyOpLogicalOperator(WeakLogicalOperator self, LogicalOperator child, /* params */);

    static TypedLogicalOperator<MyOpLogicalOperator> create(/* params */);

    [[nodiscard]] std::string_view getName() const noexcept;   // return NAME
    [[nodiscard]] Schema<Field, Unordered> getOutputSchema() const;
    [[nodiscard]] MyOpLogicalOperator withInferredSchema() const;

    [[nodiscard]] TraitSet getTraitSet() const;
    [[nodiscard]] MyOpLogicalOperator withTraitSet(TraitSet traitSet) const;

    [[nodiscard]] std::vector<LogicalOperator> getChildren() const;
    [[nodiscard]] MyOpLogicalOperator withChildren(std::vector<LogicalOperator> children) const;
    [[nodiscard]] MyOpLogicalOperator withChildrenUnsafe(std::vector<LogicalOperator> children) const;

    [[nodiscard]] std::string explain(ExplainVerbosity verbosity, OperatorId) const;
    [[nodiscard]] bool operator==(const MyOpLogicalOperator& rhs) const;

private:
    static constexpr std::string_view NAME = "MyOp";
    std::optional<LogicalOperator> child;
    TraitSet traitSet;
    // ... operator-specific fields
};
static_assert(LogicalOperatorConcept<MyOpLogicalOperator>);
```

**Physical** (`PhysicalOperatorConcept` — `nes-physical-operators/include/PhysicalOperator.hpp`):

```cpp
class MyOpPhysicalOperator final : public PhysicalOperatorConcept {
public:
    explicit MyOpPhysicalOperator(/* params */);

    void setup(ExecutionContext& ctx, CompilationContext& cctx) const override;  // optional
    void open(ExecutionContext& ctx, RecordBuffer& rb) const override;           // optional
    void execute(ExecutionContext& ctx, Record& record) const override;          // main processing
    void close(ExecutionContext& ctx, RecordBuffer& rb) const override;          // optional
    void terminate(ExecutionContext& ctx) const override;                        // optional, for state cleanup

    [[nodiscard]] std::optional<PhysicalOperator> getChild() const override;
    void setChild(PhysicalOperator child) override;

private:
    std::optional<PhysicalOperator> child;
    // ... operator-specific fields (e.g. PhysicalFunction predicate)
};
```

**Lowering rule** (`AbstractLoweringRule` — `nes-query-compiler/private/LoweringRules/AbstractLoweringRule.hpp`):

```cpp
class LowerToPhysicalMyOp : public AbstractLoweringRule {
public:
    explicit LowerToPhysicalMyOp(const QueryCompilerConfiguration& conf);
    LoweringRuleResultSubgraph apply(LogicalOperator logicalOperator) override;
};
```

### CMake registration

```cmake
# nes-logical-operators/src/Operators/CMakeLists.txt
add_plugin(MyOp LogicalOperator MyOpLogicalOperator.cpp)
add_unreflection_plugin(LogicalOperator MyOp)

# nes-query-compiler/src/LoweringRules/LowerToPhysical/CMakeLists.txt
add_plugin(MyOp LoweringRule LowerToPhysicalMyOp.cpp)

# nes-physical-operators/src/CMakeLists.txt  (or subdirectory)
add_source_files(nes-physical-operators MyOpPhysicalOperator.cpp)
```

### Skeleton (based on `Selection`)

**`LowerToPhysicalMyOp.cpp`** (key parts):
```cpp
#include <LoweringRules/LowerToPhysical/LowerToPhysicalMyOp.hpp>
#include <LoweringRuleRegistry.hpp>
#include <Operators/MyOpLogicalOperator.hpp>
#include <MyOpPhysicalOperator.hpp>
#include <Functions/FunctionProvider.hpp>
#include <Traits/FieldMappingTrait.hpp>
#include <Traits/MemoryLayoutTypeTrait.hpp>
#include <Util/SchemaFactory.hpp>
#include <PhysicalOperator.hpp>

namespace NES {

LoweringRuleResultSubgraph LowerToPhysicalMyOp::apply(LogicalOperator logicalOperator) {
    const auto myOp = logicalOperator.getAs<MyOpLogicalOperator>();

    // Lower any functions the operator holds
    const auto func = QueryCompilation::FunctionProvider::lowerFunction(
        myOp->getPredicate(),
        *myOp->getChild()->getTraitSet().get<FieldMappingTrait>());

    const auto traitSet = logicalOperator.getTraitSet();
    const auto memoryLayoutType = traitSet.get<MemoryLayoutTypeTrait>()->memoryLayout;
    const auto outputSchema = createPhysicalOutputSchema(traitSet);
    const auto inputSchema  = createPhysicalOutputSchema(myOp->getChild()->getTraitSet());

    auto physicalOperator = MyOpPhysicalOperator(func);
    const auto wrapper = std::make_shared<PhysicalOperatorWrapper>(
        physicalOperator,
        inputSchema, outputSchema,
        memoryLayoutType, memoryLayoutType,
        PhysicalOperatorWrapper::PipelineLocation::INTERMEDIATE);

    std::vector leaves(logicalOperator.getChildren().size(), wrapper);
    return {.root = wrapper, .leaves = {leaves}};
}

// Registry entry — name must match add_plugin() first argument
std::unique_ptr<AbstractLoweringRule>
LoweringRuleGeneratedRegistrar::RegisterMyOpLoweringRule(LoweringRuleRegistryArguments argument) {
    return std::make_unique<LowerToPhysicalMyOp>(argument.conf);
}
} // namespace NES
```

---

## 3. New Source

### What it is

A **source** ingests raw data into the system. It runs on a dedicated thread (`SourceThread`) and fills
`TupleBuffer`s that are pushed into the pipeline. Sources are identified by a string type name
(e.g. `"File"`, `"TCP"`, `"Generator"`).

A source can be registered as a **built-in** (compiled into `nes-sources`) or as a **plugin**
(compiled into `nes-plugins/Sources/<Name>` and activated via `activate_optional_plugin`).

### Files to create

| File | Purpose |
|------|---------|
| `nes-sources/private/MySource.hpp` (or `nes-plugins/Sources/MySource/MySource.hpp`) | Source header + config params |
| `nes-sources/src/MySource.cpp` (or `nes-plugins/Sources/MySource/MySource.cpp`) | Source implementation + registry entries |

### Interface to implement

(`Source` — `nes-sources/include/Sources/Source.hpp`):

```cpp
class MySource final : public Source {
public:
    static constexpr std::string_view NAME = "MySourceName";   // registry key

    explicit MySource(const SourceDescriptor& sourceDescriptor);
    ~MySource() override = default;

    MySource(const MySource&) = delete;
    MySource& operator=(const MySource&) = delete;
    MySource(MySource&&) = delete;
    MySource& operator=(MySource&&) = delete;

    // Called once before the ingestion loop; open connections/files here
    void open(std::shared_ptr<AbstractBufferProvider> bufferProvider) override;
    // Called once when the source stops; close connections/files here
    void close() override;
    // Called repeatedly; fill tupleBuffer with raw bytes; return eos() at end-of-stream
    FillTupleBufferResult fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stopToken) override;

    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);
    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;

private:
    // source-specific state (file handle, socket fd, etc.)
};

// Configuration parameter definition
struct ConfigParametersMySource {
    static inline const DescriptorConfig::ConfigParameter<std::string> MY_PARAM{
        "MY_PARAM",
        std::nullopt,   // no default; use std::optional<T>{value} for a default
        [](const std::unordered_map<std::string, std::string>& config) {
            return DescriptorConfig::tryGet(MY_PARAM, config);
        }};

    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap =
        DescriptorConfig::createConfigParameterContainerMap(SourceDescriptor::parameterMap, MY_PARAM /*, ... */);
};
```

### CMake registration

```cmake
# Built-in: nes-sources/src/CMakeLists.txt
add_plugin(MySourceName Source           MySource.cpp)
add_plugin(MySourceName SourceValidation MySource.cpp)

# Plugin: nes-plugins/Sources/MySource/CMakeLists.txt
add_plugin_as_library(MySourceName Source           my_source_lib     MySource.cpp)
add_plugin_as_library(MySourceName SourceValidation my_source_val_lib MySource.cpp)
target_include_directories(my_source_lib     PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_include_directories(my_source_val_lib PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
# Then in nes-plugins/CMakeLists.txt:
# activate_optional_plugin("Sources/MySource" ON)
```

### Skeleton (based on `FileSource`)

**`MySource.cpp`** (key parts):
```cpp
#include <MySource.hpp>
#include <SourceRegistry.hpp>
#include <SourceValidationRegistry.hpp>
#include <Configurations/Descriptor.hpp>
#include <ErrorHandling.hpp>

namespace NES {

MySource::MySource(const SourceDescriptor& sourceDescriptor)
    : myParam(sourceDescriptor.getFromConfig(ConfigParametersMySource::MY_PARAM))
{}

void MySource::open(std::shared_ptr<AbstractBufferProvider>) {
    // TODO: open file / connect socket / initialize hardware
}

void MySource::close() {
    // TODO: close file / disconnect socket / release hardware
}

Source::FillTupleBufferResult MySource::fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stopToken) {
    if (stopToken.stop_requested())
        return FillTupleBufferResult::eos();

    // TODO: read raw bytes into tupleBuffer.getAvailableMemoryArea<char>()
    // Return FillTupleBufferResult::withBytes(n) or FillTupleBufferResult::eos()
    return FillTupleBufferResult::eos();
}

DescriptorConfig::Config MySource::validateAndFormat(std::unordered_map<std::string, std::string> config) {
    return DescriptorConfig::validateAndFormat<ConfigParametersMySource>(std::move(config), NAME);
}

std::ostream& MySource::toString(std::ostream& str) const {
    return str << "MySource(myParam=" << myParam << ")";
}

// --- Registry entries (names must match add_plugin() first argument) ---

SourceValidationRegistryReturnType RegisterMySourceNameValidation(SourceValidationRegistryArguments args) {
    return MySource::validateAndFormat(std::move(args.config));
}

SourceRegistryReturnType SourceGeneratedRegistrar::RegisterMySourceName(SourceRegistryArguments args) {
    return std::make_unique<MySource>(args.sourceDescriptor);
}
} // namespace NES
```

---

## 4. New Sink

### What it is

A **sink** drains tuples from the pipeline to an external destination (file, network, database, etc.).
It is an `ExecutablePipelineStage` with three lifecycle hooks: `start` / `execute` / `stop`.

Like sources, sinks can be **built-in** (in `nes-sinks`) or **plugins** (in `nes-plugins/Sinks/<Name>`).

### Files to create

| File | Purpose |
|------|---------|
| `nes-sinks/include/Sinks/MySink.hpp` (or `nes-plugins/Sinks/MySink/MySink.hpp`) | Sink header + config params |
| `nes-sinks/src/MySink.cpp` (or `nes-plugins/Sinks/MySink/MySink.cpp`) | Sink implementation + registry entries |

### Interface to implement

(`Sink : ExecutablePipelineStage` — `nes-sinks/include/Sinks/Sink.hpp`):

```cpp
class MySink final : public Sink {
public:
    static constexpr std::string_view NAME = "MySinkName";   // registry key

    explicit MySink(BackpressureController backpressureController, const SinkDescriptor& sinkDescriptor);
    ~MySink() override = default;

    MySink(const MySink&) = delete;
    MySink& operator=(const MySink&) = delete;
    MySink(MySink&&) = delete;
    MySink& operator=(MySink&&) = delete;

    // Called once before the first execute(); open connections/files here
    void start(PipelineExecutionContext& pipelineExecutionContext) override;
    // Called once per TupleBuffer; write/send data here
    void execute(const TupleBuffer& inputTupleBuffer, PipelineExecutionContext& pipelineExecutionContext) override;
    // Called once after the last execute(); flush and close resources here
    void stop(PipelineExecutionContext& pipelineExecutionContext) override;

    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

protected:
    std::ostream& toString(std::ostream& str) const override;

private:
    // sink-specific state
};

// Configuration parameter definition
struct ConfigParametersMySink {
    static inline const DescriptorConfig::ConfigParameter<std::string> MY_PARAM{
        "MY_PARAM",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) {
            return DescriptorConfig::tryGet(MY_PARAM, config);
        }};

    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap =
        DescriptorConfig::createConfigParameterContainerMap(SinkDescriptor::parameterMap, MY_PARAM /*, ... */);
};
```

### CMake registration

```cmake
# Built-in: nes-sinks/src/CMakeLists.txt
add_plugin(MySinkName Sink           MySink.cpp)
add_plugin(MySinkName SinkValidation MySink.cpp)

# Plugin: nes-plugins/Sinks/MySink/CMakeLists.txt
add_plugin_as_library(MySinkName Sink           my_sink_lib     MySink.cpp)
add_plugin_as_library(MySinkName SinkValidation my_sink_val_lib MySink.cpp)
target_include_directories(my_sink_lib     PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_include_directories(my_sink_val_lib PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
# Then in nes-plugins/CMakeLists.txt:
# activate_optional_plugin("Sinks/MySink" ON)
```

### Skeleton (based on `VoidSink` for structure, `FileSink` for config pattern)

**`MySink.cpp`** (key parts):
```cpp
#include <MySink.hpp>
#include <SinkRegistry.hpp>
#include <SinkValidationRegistry.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>

namespace NES {

MySink::MySink(BackpressureController backpressureController, const SinkDescriptor& sinkDescriptor)
    : Sink(std::move(backpressureController))
    , myParam(sinkDescriptor.getFromConfig(ConfigParametersMySink::MY_PARAM))
{}

void MySink::start(PipelineExecutionContext&) {
    NES_DEBUG("Starting MySink");
    // TODO: open connections, files, allocate buffers
}

void MySink::execute(const TupleBuffer& inputTupleBuffer, PipelineExecutionContext&) {
    PRECONDITION(inputTupleBuffer, "Invalid input buffer in MySink");
    // TODO: process inputTupleBuffer
    // Use BufferIterator (nes-sinks/src/SinksParsing/BufferIterator.cpp) to iterate tuples
    // Use SchemaFormatter (nes-sinks/include/SinksParsing/SchemaFormatter.hpp) to format records
}

void MySink::stop(PipelineExecutionContext&) {
    NES_INFO("Stopping MySink");
    // TODO: flush remaining data, close resources
}

DescriptorConfig::Config MySink::validateAndFormat(std::unordered_map<std::string, std::string> config) {
    return DescriptorConfig::validateAndFormat<ConfigParametersMySink>(std::move(config), NAME);
}

std::ostream& MySink::toString(std::ostream& str) const {
    return str << "MySink(myParam=" << myParam << ")";
}

// --- Registry entries (names must match add_plugin() first argument) ---

SinkValidationRegistryReturnType RegisterMySinkNameValidation(SinkValidationRegistryArguments args) {
    return MySink::validateAndFormat(std::move(args.config));
}

SinkRegistryReturnType RegisterMySinkName(SinkRegistryArguments args) {
    return std::make_unique<MySink>(std::move(args.backpressureController), args.sinkDescriptor);
}
} // namespace NES
```

---

## Quick reference

| Extension | Registry key set by | Registration macro | Real example |
|-----------|--------------------|--------------------|-------------|
| Logical function | `static constexpr NAME` | `add_plugin(Name LogicalFunction file.cpp)` | `AbsoluteLogicalFunction` |
| Physical function | same `NAME` | `add_plugin(Name PhysicalFunction file.cpp)` | `AbsolutePhysicalFunction` |
| Logical operator | `static constexpr NAME` | `add_plugin(Name LogicalOperator file.cpp)` | `SelectionLogicalOperator` |
| Lowering rule | same `NAME` as operator | `add_plugin(Name LoweringRule file.cpp)` | `LowerToPhysicalSelection` |
| Source | `static constexpr NAME` | `add_plugin(Name Source file.cpp)` + `add_plugin(Name SourceValidation file.cpp)` | `FileSource` |
| Sink | `static constexpr NAME` | `add_plugin(Name Sink file.cpp)` + `add_plugin(Name SinkValidation file.cpp)` | `VoidSink` / `FileSink` |

---

## 5. Systest Templates

Use these templates when generating `.test` files. Substitute `<Name>`, `<name_lower>` (lowercase name),
`<NAME>` (UPPERCASE name), `<category_lower>` (category with "Functions" suffix stripped and lowercased,
e.g. `ArithmeticalFunctions` → `arithmetical`).

For functions with `## Examples`: replace the `TODO: ... = ?` lines with the actual computed values.
For sources/sinks with `## Config Parameters`: expand the SET(...) block with the real param names and
placeholder values.

### Source systest

**File**: `nes-systests/sources/<Name>.test`

```
# name: sources/<Name>.test
# description: Placeholder test for <Name> — TODO: verify records are ingested correctly
# groups: [Sources, <Name>]

# TODO: If <Name> requires external infrastructure (broker, server, device), describe setup here.

CREATE LOGICAL SOURCE <name_lower>Input(
    # TODO: Match this schema to the actual data produced by <Name>
    id UINT64 NOT NULL,
    value FLOAT64 NOT NULL
);

CREATE PHYSICAL SOURCE FOR <name_lower>Input TYPE <Name>
SET(
    # TODO: Fill in config parameters — see ConfigParameters<Name> in <Name>.hpp
    'placeholder' AS "SOURCE".PARAM_NAME
);

CREATE SINK output(id UINT64 NOT NULL, value FLOAT64 NOT NULL) TYPE File;

SELECT <name_lower>Input.id, <name_lower>Input.value
FROM <name_lower>Input
INTO output;
----
# TODO: Add expected output rows in CSV format, one per line
# 1,1.5
# 2,2.5
```

### Sink systest

**File**: `nes-systests/sinks/<Name>.test`

```
# name: sinks/<Name>.test
# description: Placeholder test for <Name> — TODO: verify records reach the destination
# groups: [Sinks, <Name>]

# TODO: If <Name> requires external infrastructure, describe setup here.

CREATE LOGICAL SOURCE input(id UINT64 NOT NULL, value FLOAT64 NOT NULL);
CREATE PHYSICAL SOURCE FOR input TYPE File;
ATTACH INLINE
1,1.5
2,2.5
3,3.5

CREATE SINK <name_lower>Output(id UINT64 NOT NULL, value FLOAT64 NOT NULL) TYPE <Name>
SET(
    # TODO: Fill in config parameters — see ConfigParameters<Name> in <Name>.hpp
    'placeholder' AS "SINK".PARAM_NAME
);

SELECT input.id, input.value
FROM input
INTO <name_lower>Output;
----
# TODO: If <Name> writes to a local file, add expected CSV output here.
# If it writes to a remote destination (network, cloud), leave empty and verify externally.
```

### Function systest

**File**: `nes-systests/function/<category_lower>/<Name>Function.test`

Strip `Functions` suffix from `category` and lowercase: `ArithmeticalFunctions` → `arithmetical`,
`BooleanFunctions` → `boolean`, `ComparisonFunctions` → `comparison`.

```
# name: function/<category_lower>/<Name>Function.test
# description: Test <NAME> function
# groups: [Function, Function<Name>]

CREATE LOGICAL SOURCE stream(
    # TODO: Define input fields matching the function's expected argument types
    x FLOAT64 NOT NULL
);
CREATE PHYSICAL SOURCE FOR stream TYPE File;
ATTACH INLINE
# TODO: Add representative input values, one per line
0.0
1.0
-1.0

CREATE SINK result(
    # TODO: Match output type from <Name>LogicalFunction::withInferredDataType()
    y FLOAT64 NOT NULL
) TYPE File;

SELECT <NAME>(x) AS y FROM stream INTO result;
----
# TODO: Replace these lines with the actual computed output values
# <NAME>(0.0) = ?
# <NAME>(1.0) = ?
# <NAME>(-1.0) = ?
```

### Operator systest

**File**: `nes-systests/operator/<name_lower>/<Name>Basic.test`

```
# name: operator/<name_lower>/<Name>Basic.test
# description: Basic placeholder test for <Name> operator
# groups: [<Name>]

CREATE LOGICAL SOURCE stream(id UINT64 NOT NULL, value FLOAT64 NOT NULL);
CREATE PHYSICAL SOURCE FOR stream TYPE File;
ATTACH INLINE
# TODO: Add representative input records that exercise the operator's behavior
1,1.5
2,2.5
1,3.5
3,4.5

CREATE SINK output(id UINT64 NOT NULL, value FLOAT64 NOT NULL) TYPE File;

# TODO: Replace this placeholder SELECT with the actual query using the <Name> operator
# once the SQL syntax for this operator is defined
SELECT stream.id, stream.value FROM stream INTO output;
----
# TODO: Add expected output after the operator transforms the input
1,1.5
2,2.5
3,4.5
```
