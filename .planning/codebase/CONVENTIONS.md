# Coding Conventions

**Analysis Date:** 2026-03-13

## Naming Patterns

**Files:**
- Header files: `.hpp` extension (e.g., `ErrorHandling.hpp`, `Timer.hpp`)
- Implementation files: `.cpp` extension
- Test files: `{ComponentName}Test.cpp` (e.g., `ThreadTest.cpp`, `PagedVectorTest.cpp`)
- Include files with macro definitions: `.inc` extension (e.g., `ExceptionDefinitions.inc`)

**Classes:**
- PascalCase/UpperCamelCase (e.g., `Exception`, `Timer`, `BaseUnitTest`, `PagedVectorTest`)
- Struct names: PascalCase (e.g., `Snapshot`, `Deleter`)

**Functions and Methods:**
- camelCase/lowerCamelCase (e.g., `generateUUID()`, `stringToUUIDOrThrow()`, `createFullyQualifiedSnapShotName()`, `getThisThreadName()`)
- Private/protected methods: same camelCase, no prefix
- Getter methods: `get{PropertyName}()` or just `operator*()`

**Variables:**
- Local variables: camelCase (e.g., `numberOfItems`, `layoutType`, `bufferManager`, `nautilusEngine`)
- Member variables: camelCase, no prefix/suffix (e.g., `message`, `errorCode`, `running`, `start_p`)
- Static constants: PascalCase (e.g., `PAGE_SIZE`, `UUID_STRING_LENGTH`)
- Thread-local variables: camelCase (e.g., `WorkerNodeId`, `ThreadName`)
- Constants: SCREAMING_SNAKE_CASE or PascalCase depending on context

**Types and Typedefs:**
- PascalCase (e.g., `UUID`, `ErrorCode`, `TimeUnit`)
- Template parameters: PascalCase (e.g., `PrintTimeUnit`, `PrintTimePrecision`)

**Enums:**
- Enum class name: PascalCase (e.g., `LogLevel`, `ExecutionMode`)
- Enum values: PascalCase (e.g., `LOG_TRACE`, `INTERPRETER`, `COLUMNAR_LAYOUT`)

## Code Style

**Formatting:**
- Tool: `clang-format` (enforced automatically)
- Configuration file: `.clang-format` at repository root

**Key Formatting Settings:**
- Indentation: 4 spaces (no tabs)
- Column limit: 140 characters
- Brace style: Custom (Based on WebKit)
  - Open braces for classes, functions, structs, enums, namespaces go on next line
  - Before `catch`, `else`, lambda bodies: braces on next line
  - Control statements: braces on next line after `AfterControlStatement`
- Pointer/Reference alignment: Left-aligned (pointer `*` and reference `&` attached to type, not variable)
  - Example: `const char* what() const` not `const char *what() const`
- Space before control statement parentheses (e.g., `if (condition)`, `for (...)`)
- No space in empty parentheses
- Template declarations always break to new line

**Linting:**
- Tool: `clang-tidy` (clang-tidy-18+)
- Configuration file: `.clang-tidy` at repository root
- Approach: "Ratchet principle" - violations fixed on main become permanent errors (not just warnings)
- Currently most checks are treated as errors; see `.clang-tidy` for full list

**Example Code Structure:**
```cpp
namespace NES
{

class Exception final : public cpptrace::lazy_exception
{
public:
    Exception(std::string message, uint64_t code);

    std::string& what() noexcept;
    [[nodiscard]] const char* what() const noexcept override;
    [[nodiscard]] ErrorCode code() const noexcept;

private:
    std::string message;
    ErrorCode errorCode;
};

}
```

## Import Organization

**Order of Includes:**
1. Standard library headers: `<cstdint>`, `<memory>`, `<string>`, etc.
2. Standard library with `.h`: `<cstdlib.h>` style headers (if used)
3. Third-party headers starting with `<` (e.g., `<fmt/core.h>`, `<spdlog/logger.h>`, `<gtest/gtest.h>`)
4. Project internal headers with `<>` and project paths (e.g., `<Util/Logger/Logger.hpp>`, `<ErrorHandling.hpp>`)
5. Project local headers with `"` (relative paths within same directory)

**Configuration:**
- `SortIncludes: true` - includes are automatically sorted alphabetically within priority groups
- `#pragma once` used instead of include guards
- Includes are organized by priority categories defined in `.clang-format`

**Path Aliases:**
- Project headers are included using angle brackets from the root include path
- Example: `#include <Util/Logger/Logger.hpp>` (not `#include "../../Util/Logger/Logger.hpp"`)

**Module Structure:**
- Headers typically organized in `include/` directory matching namespace structure
- Example: `nes-common/include/Util/Logger/Logger.hpp` for `namespace NES`
- Test utilities in `nes-common/tests/Util/include/BaseUnitTest.hpp`

## Error Handling

**Exception Pattern:**
- All exceptions derive from `NES::Exception` which extends `cpptrace::lazy_exception`
- Exceptions include: message, error code, and stacktrace
- Error codes are strongly typed as `enum ErrorCode : uint16_t` defined in `ExceptionDefinitions.inc`

**Creating Exceptions:**
```cpp
throw InvalidQuery("Query is invalid");
throw InvalidQuery("Query validation failed: {}", reason);  // With format args
```

**Throwing with Error Codes:**
- Use predefined exception functions: `InvalidQuery()`, `UnknownException()`, etc.
- Macro-generated with message and optional format string support

**Catching Exceptions:**
```cpp
try {
    // operation
} catch (const Exception& ex) {
    ASSERT_EQ(ex.code(), errorCode);
    NES_ERROR("Exception: {}", ex.what());
}
```

**Preconditions and Invariants:**
- `PRECONDITION(condition, "message", args...)` - documents requirements for calling a function
- `INVARIANT(condition, "message", args...)` - checks assumptions about state
- Both compile to no-ops in Release builds (when `NO_ASSERT` is defined)
- Both call stacktrace generation and logger flush on violation

**Error Code Checking:**
- Use `ASSERT_EXCEPTION_ERRORCODE(statement, expectedCode)` macro in tests
- Automatically catches exceptions and asserts their error code

## Logging

**Framework:** spdlog (via `Util/Logger/Logger.hpp`)

**Macros:**
- `NES_TRACE(...)` - for detailed diagnostic information
- `NES_DEBUG(...)` - for debug-level messages
- `NES_INFO(...)` - for informational messages
- `NES_WARNING(...)` - for warnings
- `NES_ERROR(...)` - for errors
- All use fmt-style formatting with `{}`

**Usage Example:**
```cpp
NES_INFO("Backend: {} and compilation: {}", magic_enum::enum_name(backend), compilation);
NES_DEBUG("Timer: Trying to start an already running timer so will skip this operation");
NES_ERROR("Precondition violated: ({}): {}", condition_expr, details);
```

**Log Levels:**
- Compile-time configurable via `NES_LOG_LEVEL` CMake variable
- Debug builds: TRACE level (all logs)
- Release builds: ERROR level (only errors)
- RelWithDebInfo: WARN level
- Benchmark builds: LEVEL_NONE (no logging)
- Stack trace inclusion: Controlled by `NES_LOG_WITH_STACKTRACE` CMake option

**Initialization:**
```cpp
Logger::setupLogging("TestName.log", LogLevel::LOG_DEBUG);
Logger::getInstance()->forceFlush();
Logger::getInstance()->shutdown();
```

## Comments

**When to Comment:**
- Above class/struct definitions: explain purpose and high-level behavior
- Above complex functions: document contract, parameters, return value, exceptions
- For non-obvious algorithms or workarounds: explain why, not what
- For known limitations or future improvements

**Documentation Style:**
- Single-line comments: `//`
- Multi-line comments: `/* ... */` or multiple `//` lines
- Inline comments sparingly and only for complex logic

**Doxygen/JSDoc Style:**
- Use `/**` for doc comments above public functions/classes
- Use `@brief`, `@param`, `@return`, `@note` tags
- Example:
```cpp
/**
 * @brief Returns the string "source" concatenated with the source counter
 * @return The generated source name
 */
std::string operator*();
```

## Function Design

**Size:**
- Keep functions focused (single responsibility principle)
- Break long functions into smaller helper functions
- No specific line limit, but cognitive complexity should remain manageable

**Parameters:**
- Use `std::string` for strings that are moved or owned
- Use `std::string_view` for string parameters that are read-only and not stored
- Pass objects by `const&` unless moved
- Use `&&` for move semantics (rvalue references)
- Collection initialization with objects: prefer single parameter functions over builders when possible

**Return Values:**
- Use `std::optional<T>` for possibly-absent values (e.g., `stringToUUID()`)
- Use `[[nodiscard]]` attribute for important return values not to be ignored
- Throw exceptions instead of returning error codes for actual errors
- Example: `[[nodiscard]] ErrorCode code() const noexcept;`

**Const Correctness:**
- Mark methods `const` when they don't modify state
- Use `noexcept` for functions that don't throw

## Module Design

**Exports:**
- Header files (`*.hpp`) are the public interface
- Implementation details in `.cpp` files or private headers
- Use `namespace` for logical grouping and avoiding name collisions

**Namespace Convention:**
- Top-level: `namespace NES`
- Logical subdomains: `namespace NES::Sequencing`, `namespace NES::Testing`, etc.
- Implementation details (internal): `namespace detail` inside public namespace

**Example Structure:**
```cpp
// In ErrorHandling.hpp
namespace NES {
    enum ErrorCode : uint16_t { ... };
    class Exception final { ... };
}

// In Timer.hpp
namespace NES {
    template <typename TimeUnit = std::chrono::nanoseconds, ...>
    class Timer {
        class Snapshot { ... };
    };
}
```

**Barrel Files:**
- Not commonly used; each header is included explicitly
- Large modules have separate headers for different components

**Multiple Inheritance:**
- Acceptable for mixing interface inheritance with test base classes
- Example: `class PagedVectorTest : public Testing::BaseUnitTest, public TestUtils::NautilusTestUtils`

## Standard Library Usage

**Preferred Types:**
- `std::string` for owned strings
- `std::string_view` for read-only string references
- `std::optional<T>` for optional values
- `std::unique_ptr<T>` for exclusive ownership
- `std::shared_ptr<T>` for shared ownership
- `std::atomic<T>` for thread-safe primitives
- `std::thread` for threading (wrapped in `Thread` class)
- `std::chrono::*` for time-related operations

**C++ Standard:**
- C++23 (via `-std=c++23` compiler flag)
- C++20 features in `.clang-format` specification
- Modern C++ features encouraged (structured bindings, ranges, etc.)

---

*Convention analysis: 2026-03-13*
