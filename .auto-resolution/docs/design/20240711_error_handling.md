# The Problem
A central concern in the current phase of NebulaStream is making our worker more robust. Our handling of errors has evolved organically over the years. This design document attempts to challenge design decisions and standardize our handling of errors.

## Preface
Following Herb Sutter, we differentiate between abstract machine corruption and programming bugs, recoverable errors:
1) **Corruptions** (e.g., stack overflow) cause a corrupted state that cannot be recovered programmatically. One can use:
    - Signal handlers: handle the termination gracefully.
2) **Bugs** (e.g., out-of-bounds, null dereference) cannot be recovered either. One can use:
    - Preconditions asserts: check whether a function was correctly called.
    - Invariant asserts: check for bugs in function.
3) **Errors** occur when the function cannot do what was advertised, i.e., could not reach a successful return postcondition. One can use:
    - Exceptions: throw and catch to communicate an error to the calling code that can recover to a valid state. This is an elegant way if the error source and handling code are separated by multiple function calls (keyword: stack unwinding).
    - std::expected & std::optional: error handling in performance-critical code where errors are likely to occur frequently.

## Current Implementation

Currently, our error handling relies on using `NES_THROW_RUNTIME_ERROR("")`, a macro that throws a `RuntimeException`. Additionally, we have scattered around the codebase around 40 exception classes. We generally rely on `NES_THROW_RUNTIME_ERROR("")`, which is used about 400 times in our codebase (about half of all exception throws).

Problems with the current implementation:

**P1:** Currently, RuntimeExceptions are used for all kinds of errors. However, we might want to handle them individually. Two example cases: i) `NES_THROW_RUNTIME_ERROR("Cant start node engine");`This exception is thrown because something serious went wrong during node engine startup. Here we might want to try to restart the complete node engine. ii) `NES_THROW_RUNTIME_ERROR("ArrowSource::ArrowSource CSV reader error: " << maybe_reader.status());`
This exception is thrown when the arrow source reads a corrupted line in a CSV file. Here, we might just want to log it and resume the execution of the query. As both cases were thrown with RuntimeException, we cannot catch them individually. They only differ in the attached string. In some cases, we even do not want to throw exceptions (e.g., because of performance reasons) or handle exceptions in the same way.

**P2:** In addition to RuntimeExceptions, other exception classes are scattered throughout the codebase. Some components define their exceptions, e.g., `QueryCompilationException.hpp` and `QueryPlacementRemovalException.hpp` exist to throw for specific components; most of the code base does rely on RuntimeExceptions. 

**P3:** Exceptions are not machine readable. There is no unified way to communicate exceptions between nodes such as the coordinator and worker and between workers. This might be important in the future as some local exceptions might only be handled by remote nodes. Also, it should be easy to parse the log and determine if and which error was thrown for testing purposes.

**P4:** We do not have guidelines for error handling.

**P5:** We rarely use invariants and preconditions.

# Goals and Non-Goals
We want a foundation for our error handling with the following properties:

**G1:** A discrete set of exception types. The solution should make all exception types available in a central place, making it easy to have errors unique and meaningful. (P1, P2)

**G2:** Enable grouping exception types by their semantics, e.g., group by the high level error source. This should enable us to handle groups of semantically similar exceptions together. (P1, P2)

**G3:** Error types should be available for testing purposes for the end user and machine-readable. (P3)

**G4:** Enable us to define new exception types easily and change existing ones. (P2)

**G5:** Provide guidelines on how and when to use exceptions and other error handling mechanisms. (P4)

We do not:

**NG1:** Design how errors are handled in a distributed setting, e.g., by communicating them among nodes.

# Solution Background

To unify error handling system, many systems use error codes:

- [Postgres](https://www.postgresql.org/docs/16/errcodes-appendix.html)
- [FoundationDB](https://apple.github.io/foundationdb/api-error-codes.html)
- [Clickhouse](https://clickhouse.com/docs/en/native-protocol/server#exception)
- [DuckDB](https://github.com/duckdb/duckdb/blob/7b276746f772e036474e80f19195a0f5af8891a0/src/include/duckdb/common/exception.hpp)

SQL has defined a set of standartized error with [SQLSTATE](https://www.ibm.com/docs/de/db2/11.1?topic=messages-sqlstate) (they are batch-related, but we might want to decide to be compatible with them in the future).

Error codes force the system to have unique error semantics. Other systems show how this makes it easier to:
- Return error codes from the [`main()` function](https://github.com/ClickHouse/ClickHouse/blob/790b66d921f56fef9b1dfdefff06c32d3174a9d2/programs/local/LocalServer.cpp#L942) to enable other programs to respond appropriately.
- [Tests that correct error handling trigger specific exceptions](https://github.com/ClickHouse/ClickHouse/blob/71c7c0edbc2dc117728258c35fbb881d300359be/tests/queries/0_stateless/02922_respect_nulls_states.sql#L19). Thus we test not only the 'happy-path'.
- Make error available in [error tables](https://clickhouse.com/docs/en/operations/system-tables/errors) or [dead letter queues](https://www.confluent.io/blog/kafka-connect-deep-dive-error-handling-dead-letter-queues/). Both mechanisms make errors 'queryable' and thus let the end user react to specific errors, e.g., divide by zero errors in streaming.
- Error codes can be used to categorize errors by ranges. E.g. in Postgres:
   ```
   Class 08 — Connection Exception
   08000	connection_exception
   08003	connection_does_not_exist
   08006	connection_failure
   ```
# Our Proposed Solution

To unify our exception handling, we will start to support error codes. For now, we will group the errors by high level error sources: user config, api requests, query registration, query runtime, sink/source data, network, bugs. 
Specifically:

- **1000-1999: Configuration Errors**: Errors that hinder the normal execution due to wrong user configuration, e.g., in the YAML configuration, cmake configuration, or command line options.  
- **2000-2999: Errors during query registration**: Errors that hinder the normal execution during query registration, parsing, validation, optimization, or compilation. These errors might lead to the termination of the query registration.
- **3000-3999: Errors during query runtime**: Errors that hinder the normal execution of the query, e.g., by the buffer manager or the node engine.
- **4000-4999: Sources and sink errors**: Errors that happen during reading from sources or writing to sinks, e.g., through invalid data.
- **5000-5999: Network errors**: Errors in network communication between workers and coordinators or workers and workers.
- **6000-6999: API Errors**: Errors from invalid API usage by users or machines.
- **9000-9999: Internal errors (e.g. bugs)**: Errors that are not caused by externalities (user input, input data, network). These errors usually mean that our code has a bug. All failed asserts will trigger exception 9000.

All exceptions and corresponding error codes are in a central file, `ExceptionDefinitions.hpp`.
Exceptions are registered using the macro `EXCEPTION(name, code, description)`.
The name is used to throw and catch the exception in the code. 
The code is the unique error identifier.
The description is used during logging and printing. 
It can be appended when needed (by modifying `what()`) to add context information to the message.

*File `ExceptionDefinitions.hpp`:*
``` C++
// 1XXX Configuration Errors
EXCEPTION(InvalidConfigParameter, 1000, "invalid configuration parameter")
EXCEPTION(MissingConfigParameter, 1001, "missing configuration parameter")
EXCEPTION(CannotLoadConfig, 1002, "cannot load configuration")

// 2XXX Errors during query registration and compilation
EXCEPTION(InvalidQuerySyntax, 2000, "invalid query syntax")
EXCEPTION(UnknownSource, 2001, "unknown source specified")
EXCEPTION(CannotSerialize, 2002, "serialization failed")
EXCEPTION(CannotDeserialize, 2003, "deserialization failed")
EXCEPTION(CannotCompileQuery, 2004, "cannot compile query")
EXCEPTION(CannotInferSchema, 2005, "cannot infer schema")
EXCEPTION(InvalidPhysicalOperator, 2006, "cannot lower operator")

// 3XXX Errors during query runtime
EXCEPTION(BufferAllocationFailure, 3000, "buffer allocation failed")
EXCEPTION(JavaUDFExcecutionFailure, 3001, "java UDF execution failure")
EXCEPTION(PythonUDFExcecutionFailure, 3002, "python UDF execution failure")
EXCEPTION(CannotStartNodeEngine, 3003, "cannot start node engine")
EXCEPTION(CannotStopNodeEngine, 3004, "cannot stop node engine")

// 4XXX Errors interpreting data stream, sources and sinks 
EXCEPTION(CannotOpenSourceFile, 4000, "cannot open source file")
EXCEPTION(CannotFormatSourceData, 4001, "cannot format source data")
EXCEPTION(MalformatedTuple, 4002, "malformed tuple")

// 5XXX Network errors
EXCEPTION(CannotConnectToCoordinator, 5000, "cannot connect to coordinator")
EXCEPTION(CoordinatorConnectionLost, 5001, "lost connection to coordinator")

// 6XXX API error
EXCEPTION(BadRequest, 6000, "invalid request")
EXCEPTION(UnauthorizedRequest, 6001, "unauthorized request")

// 9XXX Internal errors (e.g. bugs)
EXCEPTION(InternalErrorAssert, 9000, "internal error: assert failed")
EXCEPTION(FunctionNotImplemented, 9001, "function not implemented")
EXCEPTION(UnknownWindowingStrategy, 9002, "unknown windowing strategy")
EXCEPTION(UnknownWindowType, 9003, "unknown window type")
EXCEPTION(UnknownPhysicalType, 9004, "unknown physical type")
EXCEPTION(UnknownJoinStrategy, 9005, "unknown join strategy")
EXCEPTION(UnknownPluginType, 9006, "unknown plugin type")
EXCEPTION(DeprecatedFeature, 9007, "deprecated operator used")
```

`Exception.hpp` provides the central exception class and a helper macro. 

*File `Exception.hpp`:*
``` C++
class Exception {
public:
    Exception(std::string message, 
              const uint16_t code,
              const std::source_location& loc,
              const std::string& trace)
      :message(message), code(code), location_(loc), stacktrace_(trace){}

    std::string& what() { return message; }
    const std::string& what() const noexcept { return message; }
    const uint16_t code() const { return errorCode; }
    const std::source_location& where() const { return location_; }
    const std::string& stack() const { return stacktrace_; }
    
private:
    std::string message;
    uint16_t code;
    const std::source_location location_;
    const std::string stacktrace_;
};

#define EXCEPTION(name, code, message)   \
	inline Exception name(const std::source_location& loc = std::source_location::current(),  \
			              std::string trace = collectStacktrace()) {  \
		return Exception(message, code, loc, trace);  \
	}; \
	namespace ErrorCode { enum { name = code }; }
```

We provide `tryLogCurrentException()`to log the current exception and `getCurrentExceptionCode()` to get its error code:
``` C++
inline void tryLogCurrentException() {
   NES_ERROR("failed to process with error code ({}) : {}\n {}\n", e.code(), e.what(), e.where());
}
```

Which prints:
```
failed to process with code (1000) : invalid config parameter "CoordinatorID"
/Configuration.cpp(76:69), function `void ParseYAML(std::string)`
```

## Example Usage

`CannotConnectToCoordinator` is defined above in the `ExceptionDefinitions.hpp` as shown above. This enables us to throw exceptions like this:
``` C++
/* Code tying to connect to the coordinator */
if (!connected) {
    throw CannotConnectToCoordinator();
}
```

At the level in the stack where we want to add additional context information:
``` C++
try {
    /* code from above that throws CannotConnectToCoordinator */ 
} catch (const Exception& e) {    
    if (e.code() == ErrorCode::CannotConnectToCoordinator) {
        e.what() += " for query " + queryId;
	throw;
    } else if (e.code < 1000) {
	/** other error handling for configuration errors **/
    }
}
```

At the level in the stack where we want/can resume the exceution:
``` C++
try {
    /* code from above that throws CannotConnectToCoordinator */ 
} catch (const Exception& e) {
    if (e.code() == ErrorCode::CannotConnectToCoordinator) {
        /* handle exception, e.g. restore state, try to reconnect, etc. */
        tryLogCurrentException();
    }
}
```

## Guidelines

For each new function, the following flow chart should be followed:

``` mermaid
%%{init: {"flowchart": {"htmlLabels": false}} }%%
flowchart TD
    A(["`**POV: You have implemented a new function.**`"])
    G{{"`Are there calls of the function that we must not make, specifically,  certain combinations of function arguments/system states?`"}}
    H("`**Add asserts for preconditions. Document them.** They notify bugs in the caller - it should not make the call.`")
    Z{{"`Are there places in your function where you can add checks if its logic works correctly?`"}}
    U("`**Add asserts for invariants.** They notify about bugs in the function code.`")
    X{{"`Are there cases where preconditions and invariants are met, but the function cannot do what was promised? An error condition that must be reported to the calling code path in order to resolve it?`"}}
    C["`You have to add error handling to recover from that error. Is it a performance critical code path?`"]
    O["`**Throw the exception in your function.** Check if it is caught and handled appropriately.`"]
    P["`**Add a new exception type.** Throw the exception in your function and catch and handle it appropriately.`"]
    D{{"`Can you recover from that error at the caller? Can you use std::optional or std::expected as return values?`"}}
    K["`**Return std::optional or std::expected. Handle the return value appropriately.**`"]
    F{{"`Use exceptions. Check the coding guidelines and the exception list: Do you need to create a new exception type?`"}}
    S["`**Implement test cases that trigger the exception.**`"]

    A --> i
    subgraph i ["`**1. Preconditions**`"]
    G
    G -->|Yes|H
    end
    H --> j
    G --> |No| j
    subgraph j ["`**2. Invariants**`"]
    Z
    Z --> |Yes| U
    end
    U --> u
    Z --> |No| u
    subgraph u ["`**3. Recoverable Errors**`"]
    X
    X --> |Yes| C
    C -->|No|F
    C -->|Yes| D
    D --> |No|F
    D --> |Yes| K
    F --> |Yes| P
    F --> |No| O
    O --> S
    P --> S
    end
```

## How to handle errors?
- NebulaStream follows the approach of using a system-wide exception model. We support multiple exception types, and they are defined in a central file system-wide.
- All currently supported exceptions are in `ExceptionDefinitions.hpp`. Reuse existing exceptions to handle your error.
- Generally, check if an exception is needed (*exceptional cases*). Throwing and catching exceptions is [very](https://www.open-std.org/jtc1/sc22/wg21/docs/papers/2022/p2544r0.html) [expensive](https://lemire.me/blog/2022/05/13/avoid-exception-throwing-in-performance-sensitive-code/).
    - Throw if the function cannot do what is advertised.
    - Throw if the function cannot handle the error properly on its own because it needs more context.
    - In performance-sensitive code paths, ask yourself: Can your error be resolved by returning a `std::optional` or a `std::expected` instead?
- Use asserts to check preconditions and invariants of functions. We exclude asserts in our code if not compiled with `DEBUG` mode.
- Write test cases that trigger exceptions.
    ```C++
    try {
        sendMessage(someWrongNetworkConfiguration, msg);
    } catch (CannotConnectToCoordinator & e) {
        // We successfully caught the error, as expected.
        SUCCEED();
    }
    // We did not catch the error, even though we should have.
    FAIL();
    ```

## How to use exceptions?
- Throw exceptions by value.
    ```C++
    throw CannotConnectToCoordinator();
    ```
- Catch exceptions using the `const` reference.
- Rethrow with `throw;` without arguments.
- Add context to exception messages if possible. Sometimes, it makes sense to rethrow an exception and catch it where one can add more context to it. To modify the exception message later, append to the mutable string:
    ```C++
    catch (CannotConnectToCoordinator& e) {
        e.what += " for query id " + queryId;
        throw;
    }
    ```
- If a fatal error occurs, use the `main()` function return value to return the error code:
    ```C++
    int main () {
        /* ... */
        catch (...) {
            tryLogCurrentException();
            return getCurrentExceptionCode();
        }
    }
    ```

## How to define new exceptions?
- All our exceptions are in a central file, `ExceptionDefinitions.hpp`. Do not define them somewhere else.
- An exception type represents an 'error source' that needs to be handled in a specific way. Each exception type should represent a unique error requiring a specific handling approach. Thus, a new exception type should only be created if the error must be handled differently than all existing exceptions. This enables the reuse of handling methods.
- The exception name and description should very concisely describe i) which condition led to the error ("UnknownSourceType") or ii) which operation failed ("CannotConnectToCoordinator"). The former is preferred to the latter.
- When writing error messages (which are `description` and optional context), we follow the [Postgres Error Message Style Guide](https://www.postgresql.org/docs/current/error-style-guide.html).

# Alternatives

## Exception Class Hierarchies

Instead of centralizing all exception handling into a single class, we can implement a hierarchy of exception classes. This would allow us to create specific exception types that inherit from a common base class, enabling more granular control over exception handling. For example, we could have a base class `QueryException` with subclasses like `QueryCompilationException` and `QueryRuntimeException`. This approach facilitates the categorization of errors and can make the code readable and maintainable.

However, we did not choose this approach because it can lead to a complex and deeply nested class structure, making it harder to manage and extend. Moreover, ensuring that each new exception type fits correctly into the hierarchy can be difficult, especially when the codebase evolves. A centralized exception handling system with error codes is simpler to maintain and can easily accommodate new error types without the need for extensive modifications to the class hierarchy.

# Appendix

## Current Error Handling in NebulaStream
NebulaStream has mechanisms to handle exceptions and signals. While exceptions are thrown programmatically by the developers, singnals are triggered by the operating system (e.g., out of memory).

**Exceptions:**

Currently, we have a macro `NES_THROW_RUNTIME_ERROR`, which takes the current stack trace and location and throws a `RuntimeException.`

`RuntimeException`:

```C++
class RuntimeException : virtual public std::exception {
  protected:
    std::string errorMessage;
  public:
    explicit RuntimeException(std::string msg,
                              std::string&& stacktrace = collectStacktrace(),
                              const std::source_location location = std::source_location::current());
    explicit RuntimeException(std::string msg, const std::string& stack trace);
    ~RuntimeException() noexcept override = default;

    [[nodiscard]] const char* what() const noexcept override;
};
```
On construction, RuntimeException prints the error using `NES_ERROR`. It inherits from std::exception. 
Besides `RuntimeException`, we have ~40 exception classes.

An exception that is not handled by a catch calls `std::terminate`. `void nesTerminateHandler()` which invokes `invokeErrorHandlers` to notify all error listeners and `std:exit(1)`.

Main classes like the `Coordinator.hpp` and the `Worker.hpp` inherit from `ErrorListener` to be error listeners. Like this, they can implement the callback `onFatalException(std::shared_ptr<std::exception>, std::string)`. The worker uses this to notify the coordinator of the error before shutting down.


**Signals:**

Analog to exception handling classes that inherit from `ErrorListener` implement `onFatalError(int signalNumber, std::string)`.

**Implementation changes with this design document:**
- Replace all exception classes with the centralized one above (`Exception.hpp`).
- Adjust current error handling to follow the guidelines.
- Let all main classes in their ´main()´ function return the error codes.

# Sources and Further Readings

## General

- [Exceptionally Bad: The Misuse of Exceptions in C++ & How to Do Better - Peter Muldoon - CppCon 2023
](https://www.youtube.com/watch?v=Oy-VTqz1_58&ab_channel=CppCon)

- [DuckDB Exception Handling Guidelines](https://github.com/duckdb/duckdb/blob/7b276746f772e036474e80f19195a0f5af8891a0/CONTRIBUTING.md?plain=1#L57)

- [Clickhouse Exception Handling Guidelines](https://clickhouse.com/docs/en/development/style#how-to-write-code)

- [CppCoreGuidelines Exception Handling](https://isocpp.github.io/CppCoreGuidelines/CppCoreGuidelines#e15-throw-by-value-catch-exceptions-from-a-hierarchy-by-reference)

- [Fail Fast - Jim Shore](https://martinfowler.com/ieeeSoftware/failFast.pdf)

- [Avoid exception throwing in performance-sensitive code - Daniel Lemire](https://lemire.me/blog/2022/05/13/avoid-exception-throwing-in-performance-sensitive-code/)

- [Making exceptions more affordable and useable](https://accu.org/conf-docs/PDFs_2019/herb_sutter_-_de-fragmenting_cpp__making_exceptions_more_affordable_and_usable.pdf)

- [C++ exceptions are becoming more and more problematic - Thomas Neumann](https://www.open-std.org/jtc1/sc22/wg21/docs/papers/2022/p2544r0.html)

## Error Codes

- [Postgres Error Style Guide](https://www.postgresql.org/docs/current/error-style-guide.html)

- [SQLSTATE Defintion](https://www.ibm.com/docs/de/db2/11.1?topic=messages-sqlstate)

- [ClickHouse Error Tables](https://clickhouse.com/docs/en/operations/system-tables/errors)

## Std Lib Support

- [std::error_codes Guide](http://blog.think-async.com/2010/04/system-error-support-in-c0x-part-1.html)

- [Summary of SG14 discussion on `<system_error>`](https://open-std.org/jtc1/sc22/wg21/docs/papers/2018/p0824r1.html) 

