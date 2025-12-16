# How to add a `Function` in NebulaStream

Functions in NebulaStream are used to transform data within query plans. They can operate on various data types and are used in SQL expressions like `SELECT TRIM(name) FROM stream`.

Functions in NebulaStream follow a two level architecture:
1. **Logical Functions**: represent functions in the logical query plan, handling type inference and validation.
2. **Physical Functions**: execute the actual computation at runtime using the Nautilus.

Below guide will walk you through implementing a function using the `TRIM` function as an example. TRIM removes leading and trailing whitespace from variable-sized strings (VARSIZED).

## 1. Overview

Functions are implemented in two locations:
- **Logical functions**: `nes-logical-operators/src/Functions/` and `nes-logical-operators/include/Functions/`
- **Physical functions**: `nes-physical-operators/src/Functions/` and `nes-physical-operators/include/Functions/`

For our TRIM example, we create:
```
nes-logical-operators/
|── include/Functions/
    ── TrimLogicalFunction.hpp
|── src/Functions/
    ── TrimLogicalFunction.cpp

nes-physical-operators/
|── include/Functions/
    ── TrimPhysicalFunction.hpp
|── src/Functions/
    ── TrimPhysicalFunction.cpp
```

As a last step before build, you'll need to:
- register the function in CMakeLists.txt files
- create system tests in `nes-systests/function/`

## 2. Logical Function 

A logical function class must inherit from `LogicalFunctionConcept` and implement all required methods. Reference existing functions like `ConcatLogicalFunction` or `TrimLogicalFunction` for complete examples.

### 2.1 Key Components

**Header file** (`TrimLogicalFunction.hpp`):
- inherit from `LogicalFunctionConcept`
- define `static constexpr std::string_view NAME = "Trim"` (this should match SQL function name)
- store child functions and data type as private members
- declare all virtual methods from `LogicalFunctionConcept`
- add `FMT_OSTREAM(NES::TrimLogicalFunction)` at the end for logging support

**Implementation file** (`TrimLogicalFunction.cpp`):
- Constructor: initialize with child function(s) and infer data type from children
- withInferredDataType: validate argument types here (e.g., TRIM requires VARSIZED). This is where type checking should occur, not in the registry function.
- withChildren: validate the number of children matches function requirements
- serialize: serialize the function for query plan transmission (usually the same for all functions)
- Register function: implement `LogicalFunctionGeneratedRegistrar::Register{FunctionName}LogicalFunction` following the exact naming pattern

### 2.2 Notes

- Type validation: do not validate data types in the registry function (`RegisterTrimLogicalFunction`), as types may be `UNDEFINED` during registration. Validation should occur in `withInferredDataType()`.
- Error handling: always validate the number of arguments in `withChildren()` and types in `withInferredDataType()`.

## 3. Physical Function

A physical function class must inherit from `PhysicalFunctionConcept` and implement the `execute` method. Reference existing functions like `ConcatPhysicalFunction` or `TrimPhysicalFunction` for complete examples.

### 3.1 Key Components

**Header file** (`TrimPhysicalFunction.hpp`):
- inherit from `PhysicalFunctionConcept`
- declare constructor taking `PhysicalFunction` child(ren)
- implement `execute(const Record& record, ArenaRef& arena) const override`
- store child physical functions as private members

**Implementation file** (`TrimPhysicalFunction.cpp`):
- Constructor: store child physical function(s) using `std::move`
- execute: implement the actual computation logic using the Nautilus API (core part of your contribution as other steps majorly similar to each other)
- Register function: implement `PhysicalFunctionGeneratedRegistrar::Register{FunctionName}PhysicalFunction` following the exact naming pattern

## 4. CMake Registration

### 4.1 Logical Function Registration

Add to `nes-logical-operators/src/Functions/CMakeLists.txt`:

```cmake
add_plugin(Trim LogicalFunction nes-logical-operators TrimLogicalFunction.cpp)
```

### 4.2 Physical Function Registration

Add to `nes-physical-operators/src/Functions/CMakeLists.txt`:

```cmake
add_plugin(Trim PhysicalFunction nes-physical-operators TrimPhysicalFunction.cpp)
```

The `add_plugin` macro automatically will register the function in the registry (so your function will be recognized and can be called).

## 5. Testing

Create system tests in `nes-systests/function/`.

Test file structure (the first three lines are important and should be part of every test file):

```sql
# name: Trim
# description: Checks that trim in projections works as expected
# groups: [Function, Text, Projection]

CREATE LOGICAL SOURCE nameStream(fullName VARSIZED);
CREATE PHYSICAL SOURCE FOR nameStream TYPE File;
ATTACH INLINE
 Edgar Codd
        Jim Grey
Michael Stonebraker

CREATE SINK fullNameTrimmedSink(fullNameTrimmed VARSIZED) TYPE File;

SELECT TRIM(fullName) as fullNameTrimmed FROM nameStream INTO fullNameTrimmedSink;
----
Edgar Codd
Jim Grey
Michael Stonebraker

```
See `docs/development/systests.md` for more details on writing system tests.

## 6. Common Patterns

### 6.1 Functions with Multiple Arguments

For functions like `CONCAT` that take multiple arguments:

```cpp
class ConcatLogicalFunction final : public LogicalFunctionConcept
{
public:
    ConcatLogicalFunction(const LogicalFunction& left, const LogicalFunction& right);
    // ...
private:
    LogicalFunction left;
    LogicalFunction right;
};
```

In `withChildren`:
```cpp
LogicalFunction ConcatLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    if (children.size() < 2)
    {
        throw InvalidQuerySyntax("CONCAT requires at least two arguments, got {}", children.size());
    }
    // Handle multiple children
}
```

### 6.2 Type Validation

Always validate argument types in `withInferredDataType`:

```cpp
LogicalFunction MyFunction::withInferredDataType(const Schema& schema) const
{
    auto newChild = child.withInferredDataType(schema);
    if (not newChild.getDataType().isType(DataType::Type::REQUIRED_TYPE))
    {
        throw CannotInferSchema("MyFunction requires a REQUIRED_TYPE argument, but got: {}", newChild.getDataType());
    }
    return withChildren({newChild});
}
```

### 6.3 Data Type Inference

For functions that change types (e.g., arithmetic operations):

```cpp
ConcatLogicalFunction::ConcatLogicalFunction(const LogicalFunction& left, const LogicalFunction& right)
    : dataType(left.getDataType().join(right.getDataType()).value_or(DataType{DataType::Type::UNDEFINED})), 
      left(left), right(right)
{
}
```

