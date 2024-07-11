## Problem
A central concern in the current phase of NebulaStream is making our worker more robust. Our handling of exceptions has evolved organically over the years. This design document attempts to challenge design decisions and standardize our handling of exceptions.

Currently, our exception handling relies on using `NES_THROW_RUNTIME_ERROR("")`, a macro that throws a `RuntimeException`. Additionally, we have scattered around the codebase around 40 exception classes. We generally rely on `NES_THROW_RUNTIME_ERROR("")`, which is used about 400 times in our codebase.

Problems with the current implementation:

**P1:** Currently, RuntimeExceptions are used for all kinds of errors. However, we might want to handle them individually. Two example cases: i) `NES_THROW_RUNTIME_ERROR("Cant start node engine");`This exception is thrown because something serious went wrong during node engine startup. Here we might want to try to restart the complete node engine. ii) `NES_THROW_RUNTIME_ERROR("ArrowSource::ArrowSource CSV reader error: " << maybe_reader.status());`
This exception is thrown when the arrow source reads a corrupted line in a CSV file. Here, we might just want to log it and resume the execution of the query. As both cases were thrown with RuntimeException, we cannot catch them individually. They only differ in the attached string.

**P2:** In addition to RuntimeExceptions, other exception classes are scattered throughout the codebase. Some components define their exceptions, e.g., `QueryCompilationException.hpp` and `QueryPlacementRemovalException.hpp` exist to throw for specific components; most of the code base does rely on RuntimeExceptions. 

**P3:** There is no unified way to communicate errors between the coordinator and worker and among workers. Also, it should be easy to parse the log and determine if and which error was thrown for testing purposes.

**P4:** We do not have guidelines for exception usage.

## Goals
We want a foundation for our error handling with the following properties:

**G1:** A discrete set of exception types. The solution should make all exception types available in a central place, making it easy to have errors unique and meaningful. (P1, P2)

**G2:** Enable to group exception types by their semantics. Handle groups of exceptions in a specific way. (P1, P2)

**G3:** Error types should be available for testing purposes for the end user. (P3)

**G4:** Enables us to define new exception types easily. (P2)

**G5:** Provide easy-to-understand error messages. (P1, P3)

**G6:** Provide guidelines on how and when to use exceptions. (P4)
:
## Non-Goals
We do not:
- cover exception handling in the distributed case but limit ourselves to the single node

## Our Proposed Solution

### Solution Background

To unify error handling system, many systems use error codes:

- [Postgres](https://www.postgresql.org/docs/16/errcodes-appendix.html)
- [FoundationDB](https://apple.github.io/foundationdb/api-error-codes.html)
- [Clickhouse](https://clickhouse.com/docs/en/native-protocol/server#exception)
- [DuckDB](https://github.com/duckdb/duckdb/blob/7b276746f772e036474e80f19195a0f5af8891a0/src/include/duckdb/common/exception.hpp)

SQL has defined with [SQLSTATE](https://www.ibm.com/docs/de/db2/11.1?topic=messages-sqlstate) a set of standardized errors (they are batch-related, but we might want to decide to be compatible with them in the future).

Error codes force the system to have unique error semantics. Other systems show how this makes it easier to:
- Propagate errors to the program level by using error codes as return codes in the main function [`main()` function](https://github.com/ClickHouse/ClickHouse/blob/790b66d921f56fef9b1dfdefff06c32d3174a9d2/programs/local/LocalServer.cpp#L942).
- [Tests that throw specific exceptions](https://github.com/ClickHouse/ClickHouse/blob/71c7c0edbc2dc117728258c35fbb881d300359be/tests/queries/0_stateless/02922_respect_nulls_states.sql#L19)
- Make exceptions available in [error tables](https://clickhouse.com/docs/en/operations/system-tables/errors) or [dead letter queues](https://www.confluent.io/blog/kafka-connect-deep-dive-error-handling-dead-letter-queues/).
- Error codes can be used to categorize errors by ranges. E.g. in Postgres:
   ```
   Class 08 — Connection Exception
   08000	connection_exception
   08003	connection_does_not_exist
   08006	connection_failure
   ```

### Error Codes & Exception Declaration
To unify our exception handling, we will start to support error codes. For now, we start with the following error code ranges:

- **1000-1999: Errors of configuration**: Errors that hinder the normal execution due to wrong user configuration, e.g., in the YAML configuration, cmake configuration, or command line options.  
- **2000-2999: Errors during query registration and compilation**: Errors that hinder the normal execution during query registration or compilation. Usually, these errors lead to the termination of the query registration.
- **3000-3999: Errors during query runtime**: Errors that hinder the normal execution of the query. This usually happens because of system limitations.
- **4000-4999: Sources and sink errors**: Errors that happen during reading from sources or writing to sinks.
- **5000-5999: Network errors**: Errors in network communication between workers and coordinators or workers and workers.
- **9000-9999: Internal errors (e.g. bugs)**: Errors that are not caused by externalities. These errors usually mean that our code has a bug. All failed asserts will trigger exception 9000.

All exceptions and corresponding error codes are in a central file, `ExceptionDefinitions.hpp`. Exceptions are registered using the macro `EXCEPTION( name, code, description)`.

*File `ExceptionDefinitions.hpp`:*
``` C++

// 1XXX Errors of configuration
EXCEPTION( InvalidConfigParameter, 1000, "Invalid configuration parameter")
EXCEPTION( MissingConfigParameter, 1001, "Missing configuration parameter")
EXCEPTION( CannotLoadConfig, 1002, "Cannot load configuration")

// 2XXX Errors during query registration and compilation
EXCEPTION( IncorrectQuery, 2000, "Incorrect query syntax")
EXCEPTION( UnknownSource, 2001, "Unknown source specified")
EXCEPTION( SerializationFailed, 2002, "Serialization failed")
EXCEPTION( DeserializationFailed, 2003, "Deserialization failed")
EXCEPTION( CannotCompileQuery, 2004, "Cannot compile query")
EXCEPTION( CannotInferSchema, 2005, "Cannot infer schema")
EXCEPTION( CannotLowerOperator, 2006, "Cannot lower operator")

// 3XXX Errors during query runtime
EXCEPTION( BufferAllocationFailed, 3000, "Buffer allocation failed")
EXCEPTION( JavaUDFExcecutionFailure, 3001, "Java UDF execution failure")
EXCEPTION( PythonUDFExcecutionFailure, 3002, "Python UDF execution failure")
EXCEPTION( CannotStartNodeEngine, 3003, "Cannot start node engine")
EXCEPTION( CannotStopNodeEngine, 3004, "Cannot stop node engine")

// 4XXX Errors interpreting data stream, sources and sinks 
EXCEPTION( CannotOpenSourceFile, 4000, "Cannot open source file")
EXCEPTION( CannotFormatSourceData, 4001, "Cannot format source data")
EXCEPTION( MalformatedTuple, 4002, "Malformed tuple")

// 5XXX Network errors
EXCEPTION( CannotConnectToCoordinator, 5000, "Cannot connect to coordinator")

// 9XXX Internal errors (e.g. bugs)
EXCEPTION( InternalErrorAssertFailed, 9000, "Internal error: assert failed")
EXCEPTION( FunctionNotImplemented, 9001, "Function not implemented")
EXCEPTION( UnknownWindowingStrategy, 9002, "Unknown windowing strategy")
EXCEPTION( UnknownWindowType, 9003, "Unknown window type")
EXCEPTION( UnknownPhysicalType, 9004, "Unknown physical type")
EXCEPTION( UnknownJoinStrategy, 9005, "Unknown join strategy")
EXCEPTION( UnknownSourceGatheringMode, 9006, "Unknown source gathering mode")
EXCEPTION( UnknownPluginType, 9007, "Unknown plugin type")
EXCEPTION( DeprecatedFeature, 9008, "Deprecated operator used")
```

*File `Exception.hpp`:*
``` C++
class Exception {
public:
    Exception(std::string message, 
              const uint16_t code,
              const std::source_location& loc,
              std::stacktrace trace)
      :message(message), code(code), location_{loc}, stacktrace_{trace}{}

    std::string& what() {return message;}
    const std::string& what() const noexcept {return message;}
    const uint16_t code() const {return errorCode;}
    const std::source_location& where() const {return location_;}
    const std::stacktrace& stack() const {return stacktrace_;}
    
private:
    std::string message;
    uint16_t code;
    const std::source_location location_;
    const std::stacktrace stacktrace_;
};

#define EXCEPTION(name, code, message)   \
	inline Exception name(const std::source_location& loc = std::source_location::current(),  \
					         std::stacktrace trace = std::stacktrace::current()) {  \
		return Exception(message, code , loc, trace);  \
	}; \
	enum { _##name = code }; \
```

This enables us to throw and catch declared exceptions like this:
``` C++
try {
    throw CannotConnectToCoordinator();
} catch (const Exception& e) {    
	if (e.code() != _CannotConnectToCoordinator) {
	    /* Can you handle it? */
	}
}
```

We provide `tryLogCurrentException()`to log the current exception and `getCurrentExceptionCode()` to get its error code.:
``` C++
inline void tryLogCurrentException() {
   NES_ERROR("Failed to process with error code ({}) : {}\n {}\n", e.code(), e.what(), e.where());
}
```

Which prints:
```
Failed to process with code (1000) : Invalid config parameter
/Configuration.cpp(76:69), function `void ParseYAML(std::string)`
```

### Guidelines

When handling exceptions, the following guidelines should be followed:

1. Generally, check first if exceptions are needed (*exceptional cases*). Throwing and catching exceptions is [expensive](https://lemire.me/blog/2022/05/13/avoid-exception-throwing-in-performance-sensitive-code/).
    - Can your error be resolved by returning a std::optional or a std::expected?
    - Throw an exception when a function cannot do what is advertised.
    - Throw if the function cannot handle the error properly because it needs more context.
2. Throw exceptions by value.
    ```C++
        throw CannotConnectToCoordinator();
    ```
3. Catch exceptions by const reference.
4. Rethrow with throw without arguments.
5. Add context to exception messages if possible. To modify the exception message later, append to the mutable string:
    ```C++
    catch (const CannotConnectToCoordinator& e) {
        e.what += "for query id " + queryId;
        throw;
    }
    ```
6. Before creating a new error type, check if an existing type can handle it.
7. Use the `main()` function return value to return the error code.

    ```C++
    int main () {
    	/* ... */
    	catch (...) {
    	    tryLogCurrentException();
    	    return getCurrentExceptionCode();
    	}
    }
    ```
8.  Use asserts instead of exceptions to check pre- and post-invariants of functions. If compiled with `DEBUG` mode, we only include asserts in our code.
9.  Write test cases that that test if exceptions are thrown.

## Alternatives

### Exception Class Hierarchies

Instead of centralizing all exception handling into a single class, we can implement a hierarchy of exception classes. This would allow us to create specific exception types that inherit from a common base class, enabling more granular control over exception handling. For example, we could have a base class `QueryException` with subclasses like `QueryCompilationException` and `QueryRuntimeException`. This approach facilitates the categorization of errors and can make the code readable and maintainable.

However, we did not choose this approach because it can lead to a complex and deeply nested class structure, making it harder to manage and extend. Moreover, ensuring that each new exception type fits correctly into the hierarchy can be difficult, especially when the codebase evolves. A centralized exception handling system with error codes is simpler to maintain and can easily accommodate new error types without the need for extensive modifications to the class hierarchy.

## Appendix

### Current error handling in NebulaStream
NebulaStream has mechanisms to handle exceptions and signals.

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

An exception that is not handled by a catch calls `std::terminate`. `void nesTerminateHandler()` which invokes `invokeErrorHandlers` to notify all error listeners and `std:exit(1)`.

Main classes like the `Coordinator.hpp` and the `Worker.hpp` inherit from `ErrorListener` to be error listeners. Like this, they can implement the callback `onFatalException(std::shared_ptr<std::exception>, std::string)`. The worker uses this to notify the coordinator of the error before shutting down.

Besides `RuntimeException`, we have ~40 exception classes.

**Signals:**

Analog to exception handling classes that inherit from `ErrorListener` implement `onFatalError(int signalNumber, std::string)`

## Sources

### General

[Exceptionally Bad: The Misuse of Exceptions in C++ & How to Do Better - Peter Muldoon - CppCon 2023
](https://www.youtube.com/watch?v=Oy-VTqz1_58&ab_channel=CppCon)


[DuckDB Exception Handling Guidelines](https://github.com/duckdb/duckdb/blob/7b276746f772e036474e80f19195a0f5af8891a0/CONTRIBUTING.md?plain=1#L57)

[Clickhouse Exception Handling Guidelines](https://clickhouse.com/docs/en/development/style#how-to-write-code)

[CppCoreGuidelines Exception Handling](https://isocpp.github.io/CppCoreGuidelines/CppCoreGuidelines#e15-throw-by-value-catch-exceptions-from-a-hierarchy-by-reference)

[Fail Fast - Jim Shore](https://martinfowler.com/ieeeSoftware/failFast.pdf)

[Avoid exception throwing in performance-sensitive code - Daniel Lemire](https://lemire.me/blog/2022/05/13/avoid-exception-throwing-in-performance-sensitive-code/)

### Error Codes

https://www.postgresql.org/docs/current/error-style-guide.html

https://www.ibm.com/docs/de/db2/11.1?topic=messages-sqlstate

https://clickhouse.com/docs/en/operations/system-tables/errors
