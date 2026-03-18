# Coding Conventions

**Analysis Date:** 2026-03-14

## Naming Patterns

**Files:**
- Header files: `.hpp` extension (e.g., `DecideJoinTypes.hpp`, `Logger.hpp`)
- Source files: `.cpp` extension (e.g., `DecideJoinTypes.cpp`, `Logger.cpp`)
- Test files: `*Test.cpp` pattern (e.g., `LogicalPlanTest.cpp`, `ProjectionLogicalOperatorTest.cpp`)
- Files are PascalCase for classes/concepts, e.g., `AtomicState.hpp`, `TraitRegisty.hpp`

**Classes and Structs:**
- Class names: PascalCase (e.g., `DecideJoinTypes`, `LogicalPlan`, `ProjectionLogicalOperator`)
- Struct names: PascalCase (e.g., `TraitRegistryArguments`, `AtomicState`)

**Functions and Methods:**
- Method names: camelBack (e.g., `getPlan()`, `apply()`, `getChildren()`, `withChildren()`)
- Private methods: camelBack prefix, no underscore (e.g., `tryTransitionLocked()`)
- Free functions: camelBack (e.g., `generateUUID()`)

**Variables:**
- Local variables: camelBack (e.g., `sourceOp`, `selectionOp`, `joinStrategy`)
- Member variables: camelBack with no prefix (e.g., `state`, `mutex`, `errorCode`, `message`)
- Static constants: PascalCase (e.g., `WAIT_TIME_SETUP`)
- Function parameters: camelBack (e.g., `queryPlan`, `logicalOperator`, `joinFunction`)

**Types:**
- Template parameters: PascalCase (e.g., `FromState`, `ToState`, `State`)
- Type aliases: PascalCase (e.g., `TraitRegistryReturnType`)
- Enum values: PascalCase (e.g., `ROW_LAYOUT`, `UINT64`)

## Code Style

**Formatting:**
- Formatter: clang-format (`.clang-format` in root)
- Column limit: 140 characters
- Indentation: 4 spaces (no tabs)
- C++ Standard: C++23
- Brace style: Custom WebKit variant with specific rules:
  - Opening braces on same line as declaration
  - Else/catch on new line
  - Lambda body braces on same line

**Key clang-format settings:**
- `AlignAfterOpenBracket: AlwaysBreak` - Align arguments after opening bracket
- `BreakConstructorInitializersBeforeComma: false` - Constructor initializers on single line when possible
- `PointerAlignment: Left` - Pointers/references aligned left (e.g., `int* ptr`, `int& ref`)
- `AllowShortFunctionsOnASingleLine: InlineOnly` - Only inline definitions on single line
- `AlwaysBreakTemplateDeclarations: Yes` - Template declarations always break after template keyword
- `SortIncludes: true` - Includes sorted alphabetically within priority groups

**Linting:**
- Linter: clang-tidy (`.clang-tidy` in root)
- Philosophy: "All checks should be errors, except those that fail on main" (ratchet approach)
- `WarningsAsErrors` list is maintained to gradually tighten quality
- Header filter: Only checks files in `nes-*` directories
- Naming conventions enforced:
  - Classes/Structs: `CamelCase`
  - Local variables: `camelBack`
  - Static constants: `CamelCase`
  - Methods: `camelBack`
  - Type parameters: `CamelCase`

## Include Organization

**Order:**
1. Standard library headers (`<cstdint>`, `<memory>`, `<vector>`)
2. Standard library with `.h` suffix (`<pthread.h>`, etc.)
3. Project headers with common/ext/daemon prefixes
4. Project headers with `DB/` prefix
5. Project headers with `Poco/` prefix
6. Other project headers
7. External library headers

**Path Aliases:**
- Includes use angle brackets with relative paths from include directories
- Examples: `#include <Plans/LogicalPlan.hpp>`, `#include <Util/Logger/Logger.hpp>`
- No `../` relative paths used

**Include guards:**
- Use `#pragma once` (not traditional header guards)

## Error Handling

**Patterns:**

**Exception throwing:**
```cpp
throw DecideJoinTypes("Invalid join configuration");
throw Exception("Custom message", ErrorCode::UnknownException);
```

**Exception catching and handling:**
```cpp
try {
    statement;
} catch (const Exception& ex) {
    NES_ERROR("Error: {}", ex.what());
}
```

**Preconditions and Invariants:**
- Use `PRECONDITION(condition, "message")` macro to check function entry contracts
- Use `INVARIANT(condition, "message")` macro to check internal state consistency
- Both macros are no-op in Release builds (`NO_ASSERT` defined)
- Example:
  ```cpp
  PRECONDITION(queryPlan.getRootOperators().size() == 1, "Only single root operators supported");
  INVARIANT(not queryPlan.getRootOperators().empty(), "Query must have a sink root operator");
  ```

**Error codes:**
- Defined in `ExceptionDefinitions.inc`
- Used via enum: `ErrorCode::CannotInferSchema`
- Each error has a code (uint16_t) and message

## Logging

**Framework:** spdlog with compile-time optimization

**Patterns:**
- Use compile-time log level filtering via `NES_COMPILE_TIME_LOG_LEVEL`
- Log macros: `NES_TRACE()`, `NES_DEBUG()`, `NES_INFO()`, `NES_WARN()`, `NES_ERROR()`
- Format strings use `fmt` library format (e.g., `NES_ERROR("Message: {}", value)`)
- Example:
  ```cpp
  NES_DEBUG("Setup QueryPlanTest test class.");
  NES_ERROR("Precondition violated: ({}): " formatString, condition);
  ```

**Log levels (compile-time configurable):**
- `NES_LOGLEVEL_TRACE` - Everything
- `NES_LOGLEVEL_DEBUG` - Debug messages
- `NES_LOGLEVEL_INFO` - Information messages
- `NES_LOGLEVEL_WARN` - Warnings
- `NES_LOGLEVEL_ERROR` - Errors only
- `NES_LOGLEVEL_NONE` - No logging

**Stacktrace logging:**
- When `NES_LOG_WITH_STACKTRACE` is enabled, exceptions include stack traces
- Disabled in Thread Sanitizer (TSAN) builds due to performance

## Comments

**When to Comment:**
- Document why code does something, not what (code should be clear)
- Use for non-obvious design decisions
- Mark compiler/linter suppressions with `///NOLINT` pragma
- Document test setup/teardown behavior

**JSDoc/TSDoc style (used minimally):**
- Use `///` for documentation comments on classes/functions
- Document public APIs with parameter descriptions
- Example (from BaseUnitTest.hpp):
  ```cpp
  /// @brief Returns the string "source" concatenated with the source counter.
  /// @return std::string
  std::string operator*();
  ```

**Inline comments:**
- Use `//` for same-line or block comments
- Keep brief and to the point
- Example: `/// This deleter is used to create a shared_ptr that does not delete the object.`

## Function Design

**Size:** Keep functions focused on single responsibility
- Example: `apply()` recursively applies transformations through operator tree
- Prefer helper methods for complex operations

**Parameters:**
- Use const references for objects: `const LogicalOperator& operator`
- Pass by value for small types
- Use move semantics for ownership transfer
- Example: `LogicalPlan apply(const LogicalPlan& queryPlan);`

**Return Values:**
- Return objects directly (rely on move semantics)
- Use `std::optional` for optional results
- Use `std::vector` for collections
- Example:
  ```cpp
  LogicalOperator apply(const LogicalOperator& logicalOperator);
  std::optional<LogicalOperator> tryFind(...);
  ```

**Template usage:**
- Templates preferred for compile-time polymorphism
- Concepts used for template constraints (C++20)
- Example:
  ```cpp
  template <typename FromState, typename ToState>
  bool transition(const std::function<ToState(FromState&&)>&);
  ```

## Module Design

**Exports:**
- Classes exported via `.hpp` files in `include/` directories
- Private implementation in `private/` directories
- Example structure:
  - Public: `nes-query-optimizer/include/TraitRegistry.hpp`
  - Private: `nes-query-optimizer/private/Phases/DecideJoinTypes.hpp`

**Barrel Files:**
- Not heavily used; prefer explicit includes

**Namespaces:**
- All production code in `namespace NES`
- Test code in nested `namespace NES::Testing`
- Anonymous namespaces for internal helper functions
- Example:
  ```cpp
  namespace NES {
  namespace {
    bool shallUseHashJoin(const LogicalFunction& joinFunction) { ... }
  }
  }
  ```

**Access modifiers:**
- public/private/protected explicitly declared
- Private members use camelBack naming (no underscore prefix)
- Access modifier offset: -4 spaces (non-standard)

---

*Convention analysis: 2026-03-14*
