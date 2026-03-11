# How to add a `Function`

Functions in NebulaStream represent operations applied to data within queries, such as arithmetic (`Add`, `Mul`), comparisons (`Greater`, `Less`), boolean logic (`And`, `Or`), or more complex expressions like conditionals (`CASE WHEN`).

Each function consists of two parts:
1. **Logical function** — used during query planning for type inference, validation, and serialization.
2. **Physical function** — used at runtime to execute the function on actual data.

Both parts are registered as plugins in the plugin system and are connected through the registry.
This guide walks you through implementing both parts using the `ConditionalFunction` as an example.
It implements a SQL-style `CASE WHEN` expression: given a list of condition/result pairs and a default value, it returns the result of the first condition that evaluates to true, or the default if none match.

```sql
CASE WHEN cond1 THEN result1
     WHEN cond2 THEN result2
     ELSE default
```

Internally, the children are stored as a flat list with an odd number of elements (>= 3):
`[cond1, result1, cond2, result2, ..., condN, resultN, default]`

## 1. Overview

Function plugins live in `nes-plugins/Functions/`.
Each function gets its own directory containing the logical and physical implementations, a `CMakeLists.txt`, and optionally a `tests/` directory.

```
nes-plugins/
├── Sources/
├── Sinks/
├── Functions/
│   ├── ConditionalFunction/
│   │   ├── ConditionalLogicalFunction.hpp
│   │   ├── ConditionalLogicalFunction.cpp
│   │   ├── ConditionalPhysicalFunction.hpp
│   │   ├── ConditionalPhysicalFunction.cpp
│   │   ├── CMakeLists.txt
│   │   └── tests/
│   │       ├── ConditionalLogicalFunctionTest.cpp
│   │       ├── ConditionalPhysicalFunctionTest.cpp
│   │       └── CMakeLists.txt
│   └── ...
└── ...
```

Functions that are well-tested and widely used may be promoted to internal plugins. These live directly in the component source directories:
- Logical: `nes-logical-operators/src/Functions/`
- Physical: `nes-physical-operators/src/Functions/`

## 2. CMake

Each function plugin registers two plugins — one for the logical and one for the physical function.
In the plugin's `CMakeLists.txt`:

```cmake
add_plugin(Conditional LogicalFunction nes-logical-operators
        ConditionalLogicalFunction.cpp
)
target_include_directories(nes-logical-operators PUBLIC .)

add_plugin(Conditional PhysicalFunction nes-physical-operators
        ConditionalPhysicalFunction.cpp
)
target_include_directories(nes-physical-operators PUBLIC .)
```

The arguments to `add_plugin` are:
- `Conditional` — the plugin name, used as the registry key.
- `LogicalFunction` / `PhysicalFunction` — the registry the plugin belongs to.
- `nes-logical-operators` / `nes-physical-operators` — the component library to add the plugin to.
- The source files that make up the plugin.

To activate the plugin, add it to `nes-plugins/CMakeLists.txt`:
```cmake
activate_optional_plugin("Functions/ConditionalFunction" ON)
```

If your function needs tests, add a `tests/` directory with its own `CMakeLists.txt`:
```cmake
add_nes_test(ConditionalLogicalFunctionTest
        SOURCES ConditionalLogicalFunctionTest.cpp
        LINK nes-logical-operators
)

add_nes_test(ConditionalPhysicalFunctionTest
        SOURCES ConditionalPhysicalFunctionTest.cpp
        LINK nes-testing
)
```

## 3. Logical Function

The logical function is the compile-time/planning-time representation.
It is used to infer data types, validate the function's structure, produce human-readable explanations, and serialize/deserialize the function to send it over the wire.

### Interface

A logical function must satisfy the `LogicalFunctionConcept`, which requires the following methods:

| Method                         | Purpose                                                  |
|--------------------------------|----------------------------------------------------------|
| `getType()`                    | Returns the function's name (must match the plugin name) |
| `getDataType()`                | Returns the result data type                             |
| `withDataType(dataType)`       | Returns a copy with a different data type                |
| `withInferredDataType(schema)` | Infers and returns the function with resolved types      |
| `getChildren()`                | Returns child functions                                  |
| `withChildren(children)`       | Returns a copy with different children                   |
| `explain(verbosity)`           | Returns a human-readable string representation           |
| `operator==`                   | Equality comparison                                      |

Additionally, you must provide `Reflector` and `Unreflector` template specializations for serialization.

### Header

The header declares the class, its members, and the serialization specializations:

```cpp
class ConditionalLogicalFunction final
{
public:
    static constexpr std::string_view NAME = "Conditional";

    explicit ConditionalLogicalFunction(std::vector<LogicalFunction> children);

    [[nodiscard]] bool operator==(const ConditionalLogicalFunction& rhs) const;

    [[nodiscard]] DataType getDataType() const;
    [[nodiscard]] ConditionalLogicalFunction withDataType(const DataType& dataType) const;
    [[nodiscard]] LogicalFunction withInferredDataType(const Schema& schema) const;

    [[nodiscard]] std::vector<LogicalFunction> getChildren() const;
    [[nodiscard]] ConditionalLogicalFunction withChildren(const std::vector<LogicalFunction>& children) const;

    [[nodiscard]] std::string_view getType() const;
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const;

private:
    DataType dataType;
    std::vector<LogicalFunction> children;

    friend Reflector<ConditionalLogicalFunction>;
};

template <>
struct Reflector<ConditionalLogicalFunction>
{
    Reflected operator()(const ConditionalLogicalFunction& function) const;
};

template <>
struct Unreflector<ConditionalLogicalFunction>
{
    ConditionalLogicalFunction operator()(const Reflected& reflected) const;
};

static_assert(LogicalFunctionConcept<ConditionalLogicalFunction>);
```

The `NAME` constant must match the plugin name used in `CMakeLists.txt` (here: `"Conditional"`).
The `static_assert` at the bottom verifies at compile time that the class satisfies the concept.

### Basic Methods

Most methods are straightforward. The constructor infers its initial type from the last child (the default value).
`withChildren` simply constructs a new instance, and `getType` returns the `NAME` constant:

```cpp
ConditionalLogicalFunction::ConditionalLogicalFunction(std::vector<LogicalFunction> children)
    : dataType(children.back().getDataType()), children(std::move(children))
{
}

ConditionalLogicalFunction ConditionalLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    return ConditionalLogicalFunction(children);
}

std::string_view ConditionalLogicalFunction::getType() const
{
    return NAME;
}
```

The `explain` method produces a human-readable representation, used for query plan output:
```cpp
std::string ConditionalLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    std::string result = "CASE";
    for (size_t i = 0; i + 1 < children.size(); i += 2)
    {
        result += fmt::format(" WHEN {} THEN {}", children.at(i).explain(verbosity), children.at(i + 1).explain(verbosity));
    }
    result += fmt::format(" ELSE {}", children.back().explain(verbosity));
    return result;
}
```

### Type Inference

The `withInferredDataType` method is called during query planning to resolve types based on the input schema.
First, it recursively infers types for all children.
Then, it validates the structure: conditions (even indices) must be `BOOLEAN`, and all results (odd indices) must match the type of the default (last element):

```cpp
LogicalFunction ConditionalLogicalFunction::withInferredDataType(const Schema& schema) const
{
    std::vector<LogicalFunction> inferredChildren;
    inferredChildren.reserve(children.size());
    for (const auto& child : children)
    {
        inferredChildren.push_back(child.withInferredDataType(schema));
    }

    /// Even indexes (0, 2, 4, ...) are conditions and must be BOOLEAN.
    for (std::size_t i = 0; i + 1 < inferredChildren.size(); i += 2)
    {
        if (!inferredChildren.at(i).getDataType().isType(DataType::Type::BOOLEAN))
        {
            throw CannotInferSchema(
                "ConditionalLogicalFunction: condition at index {} must be BOOLEAN, but was: {}",
                i, inferredChildren.at(i).getDataType());
        }
    }

    /// Odd indexes (1, 3, 5, ...) are results and must have the same type as the default (last element).
    const auto& resultType = inferredChildren.back().getDataType();
    for (std::size_t i = 1; i + 1 < inferredChildren.size(); i += 2)
    {
        if (inferredChildren.at(i).getDataType() != resultType)
        {
            throw CannotInferSchema(
                "ConditionalLogicalFunction: result at index {} has type {}, but expected {} (matching default)",
                i, inferredChildren.at(i).getDataType(), resultType);
        }
    }

    return withChildren(inferredChildren).withDataType(resultType);
}
```

For simpler functions (e.g., `Add`), type inference can just recursively infer children and call `withChildren`, which recomputes the type.

### Serialization

Each logical function needs a `Reflector` (serialize) and `Unreflector` (deserialize) specialization.
Define a helper struct with `std::optional` fields for each piece of data you need to persist, then use `reflect` and `unreflect` to convert between your type and the serialized form:

```cpp
/// Helper struct for serialization (declared in the header)
namespace NES::detail
{
struct ReflectedConditionalLogicalFunction
{
    std::vector<std::optional<LogicalFunction>> children;
};
}
```

```cpp
/// Serialize
Reflected Reflector<ConditionalLogicalFunction>::operator()(const ConditionalLogicalFunction& function) const
{
    detail::ReflectedConditionalLogicalFunction reflected;
    reflected.children.reserve(function.children.size());
    for (const auto& child : function.children)
    {
        reflected.children.push_back(std::make_optional(child));
    }
    return reflect(reflected);
}

/// Deserialize
ConditionalLogicalFunction Unreflector<ConditionalLogicalFunction>::operator()(const Reflected& reflected) const
{
    auto [childrenOpts] = unreflect<detail::ReflectedConditionalLogicalFunction>(reflected);
    std::vector<LogicalFunction> children;
    children.reserve(childrenOpts.size());
    for (const auto& childOpt : childrenOpts)
    {
        if (!childOpt.has_value())
        {
            throw CannotDeserialize("ConditionalLogicalFunction child is missing");
        }
        children.push_back(childOpt.value());
    }
    if (children.size() < 3 || children.size() % 2 == 0)
    {
        throw CannotDeserialize(
            "ConditionalLogicalFunction requires an odd number of children >= 3, but got {}", children.size());
    }
    return ConditionalLogicalFunction(std::move(children));
}
```

### Registration Function

Each logical function must define its registration function.
This is called by the auto-generated registrar to construct the function from either serialized data or children passed from the query plan:

```cpp
LogicalFunctionRegistryReturnType
LogicalFunctionGeneratedRegistrar::RegisterConditionalLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        return unreflect<ConditionalLogicalFunction>(arguments.reflected);
    }
    /// Must have an odd number of children >= 3: at least one (condition, result) pair + default.
    if (arguments.children.size() < 3 || arguments.children.size() % 2 == 0)
    {
        throw CannotDeserialize(
            "ConditionalLogicalFunction requires an odd number of arguments >= 3 "
            "(condition/result pairs + default), but got {}",
            arguments.children.size());
    }
    return ConditionalLogicalFunction(std::move(arguments.children));
}
```

The function name must follow the pattern `Register{PluginName}LogicalFunction` — for a plugin named `Conditional`, this becomes `RegisterConditionalLogicalFunction`.

## 4. Physical Function

The physical function handles the actual execution at runtime.
It receives a `Record` (a row of data) and an `ArenaRef` (for memory allocation), and returns a `VarVal` (a variant value type).

### Interface

A physical function must satisfy the `PhysicalFunctionConcept`, which requires a single method:

```cpp
VarVal execute(const Record& record, ArenaRef& arena) const;
```

### Header

```cpp
class ConditionalPhysicalFunction final
{
public:
    ConditionalPhysicalFunction(std::vector<PhysicalFunction> inputFns);
    [[nodiscard]] VarVal execute(const Record& record, ArenaRef& arena) const;

private:
    std::vector<std::pair<PhysicalFunction, PhysicalFunction>> whenThenFns;
    PhysicalFunction elseFn;
};

static_assert(PhysicalFunctionConcept<ConditionalPhysicalFunction>);
```

The constructor receives the flat list of child functions and splits them into condition/result pairs plus a default.
You can organize the internal storage however you like — the flat list format is only required for the registry interface.

### Implementation

The constructor separates the flat input list into structured pairs:

```cpp
ConditionalPhysicalFunction::ConditionalPhysicalFunction(std::vector<PhysicalFunction> inputFns)
    : elseFn(inputFns.back())
{
    inputFns.pop_back();

    whenThenFns.reserve(inputFns.size() / 2);
    for (size_t i = 0; i + 1 < inputFns.size(); i += 2)
    {
        whenThenFns.emplace_back(inputFns[i], inputFns[i + 1]);
    }
}
```

The `execute` method evaluates conditions in order and returns the result of the first one that is true.
If no condition matches, it returns the default:

```cpp
VarVal ConditionalPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    for (nautilus::static_val<size_t> i = 0; i < whenThenFns.size(); ++i)
    {
        if (whenThenFns.at(i).first.execute(record, arena))
        {
            return whenThenFns.at(i).second.execute(record, arena);
        }
    }
    return elseFn.execute(record, arena);
}
```

Child functions are themselves `PhysicalFunction` instances, so they can be any other function (field access, constants, nested expressions, etc.).
`nautilus::static_val<size_t>` is used for the loop counter to allow the compiler to unroll the loop during compilation.

### Registration Function

The physical function also needs a registration function.
The `PhysicalFunctionRegistryArguments` provides:
- `childFunctions` — the already-constructed child physical functions.
- `inputTypes` — the data types of the inputs.
- `outputType` — the expected output data type.

Use these for validation at construction time:

```cpp
PhysicalFunctionRegistryReturnType
PhysicalFunctionGeneratedRegistrar::RegisterConditionalPhysicalFunction(PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(
        arguments.childFunctions.size() >= 3,
        "Conditional function must have at least 3 child functions");
    PRECONDITION(
        arguments.childFunctions.size() % 2 == 1,
        "Conditional function must have an odd number of child functions");

    const auto& inputTypes = arguments.inputTypes;
    for (size_t i = 0; i + 1 < inputTypes.size() - 1; i += 2)
    {
        PRECONDITION(inputTypes[i].isType(DataType::Type::BOOLEAN), "All condition functions must have type BOOLEAN");
    }

    const auto& outputType = inputTypes.back();
    for (size_t i = 1; i + 1 < inputTypes.size() - 1; i += 2)
    {
        PRECONDITION(inputTypes[i] == outputType, "All result functions must have the same type");
    }

    return ConditionalPhysicalFunction(std::move(arguments.childFunctions));
}
```

The function name follows the pattern `Register{PluginName}PhysicalFunction`.

## 5. Testing

### Unit Tests

You can write unit tests for both the logical and physical function.

**Logical function tests** typically cover:
- Type inference with different input types
- Validation of invalid inputs (e.g., wrong number of children, incompatible types)
- Serialization and deserialization roundtrips
- The `explain` output
- Equality comparison

**Physical function tests** typically cover:
- Correct results for different input types and values
- Behavior with nested child functions
- Both interpreted and compiled execution modes (using `InterpretationBasedExecutionContext` and `CompilationBasedExecutionContext`)

Example of a physical function test:
```cpp
TEST_F(ConditionalPhysicalFunctionTest, basicConditionalInt64)
{
    /// Build: CASE WHEN true THEN 42 ELSE 0
    auto conditionFn = PhysicalFunction(ConstantValuePhysicalFunction(true));
    auto thenFn = PhysicalFunction(ConstantValuePhysicalFunction(INT64_C(42)));
    auto elseFn = PhysicalFunction(ConstantValuePhysicalFunction(INT64_C(0)));

    auto conditionalFn = ConditionalPhysicalFunction({conditionFn, thenFn, elseFn});
    auto result = executeFn(conditionalFn);

    EXPECT_EQ(result, VarVal(INT64_C(42)));
}
```

### System Tests

System tests verify the function end-to-end within a running NebulaStream instance.
They are defined as `.test` files in `nes-systests/function/` and use a declarative format to specify input data, the query, and expected output.

## Summary

To add a new function:

1. Create a directory under `nes-plugins/Functions/` with a `CMakeLists.txt` that registers both a `LogicalFunction` and a `PhysicalFunction` plugin.
2. Activate the plugin in `nes-plugins/CMakeLists.txt`.
3. Implement the logical function satisfying `LogicalFunctionConcept`, including type inference, serialization, and the `Register...LogicalFunction` function.
4. Implement the physical function satisfying `PhysicalFunctionConcept`, including the `execute` method and the `Register...PhysicalFunction` function.
5. Write tests for both parts.
