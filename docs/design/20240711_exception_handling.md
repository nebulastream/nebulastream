## Problem
A central concern in the current phase of NebulaStream is making our worker more robust. Our handling of errors has evolved organically over the years. This design document attempts to challenge design decisions and standardize our handling of errors.

Currently, our error handling relies on using `NES_THROW_RUNTIME_ERROR("")`, a macro that throws a `RuntimeException`. Additionally, we have scattered around the codebase around 40 exception classes. We generally rely on `NES_THROW_RUNTIME_ERROR("")`, which is used about 400 times in our codebase (about half of all exception throws).

Problems with the current implementation:

**P1:** Currently, RuntimeExceptions are used for all kinds of errors. However, we might want to handle them individually. Two example cases: i) `NES_THROW_RUNTIME_ERROR("Cant start node engine");`This exception is thrown because something serious went wrong during node engine startup. Here we might want to try to restart the complete node engine. ii) `NES_THROW_RUNTIME_ERROR("ArrowSource::ArrowSource CSV reader error: " << maybe_reader.status());`
This exception is thrown when the arrow source reads a corrupted line in a CSV file. Here, we might just want to log it and resume the execution of the query. As both cases were thrown with RuntimeException, we cannot catch them individually. They only differ in the attached string.

**P2:** In addition to RuntimeExceptions, other exception classes are scattered throughout the codebase. Some components define their exceptions, e.g., `QueryCompilationException.hpp` and `QueryPlacementRemovalException.hpp` exist to throw for specific components; most of the code base does rely on RuntimeExceptions. 

**P3:** There is no unified way to communicate errors between the coordinator and worker and between workers. Also, it should be easy to parse the log and determine if and which error was thrown for testing purposes.

**P4:** We do not have guidelines for exception usage.

## Goals
We want a foundation for our error handling with the following properties:

**G1:** A discrete set of exception types. The solution should make all exception types available in a central place, making it easy to have errors unique and meaningful. (P1, P2)

**G2:** Enable grouping exception types by their semantics. Handle groups of exceptions in a specific way. (P1, P2)

**G3:** Error types should be available for testing purposes for the end user. (P3)

**G4:** Enables us to define new exception types easily and change existing ones. (P2)

**G5:** Provide guidelines on how and when to use exceptions. (P4)

## Non-Goals
We do not:
- design way in which errors are handled in a distributed setting, e.g., by communicating them among nodes.

## Our Proposed Solution

### Solution Background

To unify error handling system, many systems use error codes:

- [Postgres](https://www.postgresql.org/docs/16/errcodes-appendix.html)
- [FoundationDB](https://apple.github.io/foundationdb/api-error-codes.html)
- [Clickhouse](https://clickhouse.com/docs/en/native-protocol/server#exception)
- [DuckDB](https://github.com/duckdb/duckdb/blob/7b276746f772e036474e80f19195a0f5af8891a0/src/include/duckdb/common/exception.hpp)

SQL has defined a set of standartized error with [SQLSTATE](https://www.ibm.com/docs/de/db2/11.1?topic=messages-sqlstate) (they are batch-related, but we might want to decide to be compatible with them in the future).

Error codes force the system to have unique error semantics. Other systems show how this makes it easier to:
- Return error codes from the [`main()` function](https://github.com/ClickHouse/ClickHouse/blob/790b66d921f56fef9b1dfdefff06c32d3174a9d2/programs/local/LocalServer.cpp#L942) to enable other programs to repsond appropriately.
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

- **1000-1999: Configuration Errors**: Errors that hinder the normal execution due to wrong user configuration, e.g., in the YAML configuration, cmake configuration, or command line options.  
- **2000-2999: Errors during query registration**: Errors that hinder the normal execution during query registration, optimization, or compilation. These errors might lead to the termination of the query registration.
- **3000-3999: Errors during query runtime**: Errors that hinder the normal execution of the query, e.g., by the buffer manager or the node engine.
- **4000-4999: Sources and sink errors**: Errors that happen during reading from sources or writing to sinks, e.g., through invalid data.
- **5000-5999: Network errors**: Errors in network communication between workers and coordinators or workers and workers.
- **9000-9999: Internal errors (e.g. bugs)**: Errors that are not caused by externalities (user input, input data, network). These errors usually mean that our code has a bug. All failed asserts will trigger exception 9000.

All exceptions and corresponding error codes are in a central file, `ExceptionDefinitions.hpp`. Exceptions are registered using the macro `EXCEPTION( name, code, description)`.

*File `ExceptionDefinitions.hpp`:*
``` C++

// 1XXX Configuration Errors
EXCEPTION( InvalidConfigParameterException, 1000, "Invalid configuration parameter")
EXCEPTION( MissingConfigParameterException, 1001, "Missing configuration parameter")
EXCEPTION( CannotLoadConfigException, 1002, "Cannot load configuration")

// 2XXX Errors during query registration and compilation
EXCEPTION( InvalidQuerySyntaxException, 2000, "Invalid query syntax")
EXCEPTION( UnknownSourceException, 2001, "Unknown source specified")
EXCEPTION( CannotSerializeException, 2002, "Serialization failed")
EXCEPTION( CannotDeserializeException, 2003, "Deserialization failed")
EXCEPTION( CannotCompileQueryException, 2004, "Cannot compile query")
EXCEPTION( CannotInferSchemaException, 2005, "Cannot infer schema")
EXCEPTION( CannotLowerOperatorException, 2006, "Cannot lower operator")

// 3XXX Errors during query runtime
EXCEPTION( BufferAllocationFailureException, 3000, "Buffer allocation failed")
EXCEPTION( JavaUDFExcecutionFailureException, 3001, "Java UDF execution failure")
EXCEPTION( PythonUDFExcecutionFailureException, 3002, "Python UDF execution failure")
EXCEPTION( CannotStartNodeEngineException, 3003, "Cannot start node engine")
EXCEPTION( CannotStopNodeEngineException, 3004, "Cannot stop node engine")

// 4XXX Errors interpreting data stream, sources and sinks 
EXCEPTION( CannotOpenSourceFileException, 4000, "Cannot open source file")
EXCEPTION( CannotFormatSourceDataException, 4001, "Cannot format source data")
EXCEPTION( MalformatedTupleException, 4002, "Malformed tuple")

// 5XXX Network errors
EXCEPTION( CannotConnectToCoordinatorException, 5000, "Cannot connect to coordinator")
EXCEPTION( CoordinatorConnectionLostException, 5001, "Lost connection to cooridnator")

// 9XXX Internal errors (e.g. bugs)
EXCEPTION( InternalErrorAssertException, 9000, "Internal error: assert failed")
EXCEPTION( FunctionNotImplementedException, 9001, "Function not implemented")
EXCEPTION( UnknownWindowingStrategyException, 9002, "Unknown windowing strategy")
EXCEPTION( UnknownWindowTypeException, 9003, "Unknown window type")
EXCEPTION( UnknownPhysicalTypeException, 9004, "Unknown physical type")
EXCEPTION( UnknownJoinStrategyException, 9005, "Unknown join strategy")
EXCEPTION( UnknownPluginTypeException, 9006, "Unknown plugin type")
EXCEPTION( DeprecatedFeatureException, 9007, "Deprecated operator used")
```

*File `Exception.hpp`:*
``` C++
class Exception {
public:
    Exception(std::string message, 
              const uint16_t code,
              const std::source_location& loc,
              const std::string& trace)
      :message(message), code(code), location_{loc}, stacktrace_{trace}{}

    std::string& what() {return message;}
    const std::string& what() const noexcept {return message;}
    const uint16_t code() const {return errorCode;}
    const std::source_location& where() const {return location_;}
    const std::string& stack() const {return stacktrace_;}
    
private:
    std::string message;
    uint16_t code;
    const std::source_location location_;
    const std::string stacktrace_;
};

#define EXCEPTION(name, code, message)   \
	inline Exception name(const std::source_location& loc = std::source_location::current(),  \
			      std::stacktrace trace = collectStacktrace()) {  \
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
Failed to process with code (1000) : Invalid config parameter "CoordinatorID"
/Configuration.cpp(76:69), function `void ParseYAML(std::string)`
```

### Guidelines

When handling errors, the following guidelines should be followed:

*How to handle errors?*
- Generally, check first if exceptions are needed (*exceptional cases*). Throwing and catching exceptions is [expensive](https://lemire.me/blog/2022/05/13/avoid-exception-throwing-in-performance-sensitive-code/).
    - Throw an exception when a function cannot do what is advertised.
    - Throw if the function cannot handle the error properly because it needs more context.
    - In performance sensitive code paths, ask yourself: can your error be resolved by returning a `std::optional` or a `std::expected`?
- Use asserts to check pre- and post-invariants of functions. If compiled with `DEBUG` mode, we include asserts in our code.
-  Write test cases that trigger exceptions. If an exception cannot be easily triggered by an test case it should probably be an assert.
    ```C++
    try {
	sendMessage(wrongNetworkConfiguration, msg);
    } catch (CannotConnectToCoordinator & e) {
	SUCCEED();
    }
    FAIL();
    ```

*How to use exceptions?*
- Throw exceptions by value.
    ```C++
        throw CannotConnectToCoordinator();
    ```
- Catch exceptions by const reference.
- Rethrow with throw without arguments.
- Add context to exception messages if possible. Sometimes it makes sense to rethrow an exeption and catch it where one can add more context to it. To modify the exception message later, append to the mutable string:
    ```C++
    catch (CannotConnectToCoordinator& e) {
        e.what += "for query id " + queryId;
        throw;
    }
    ```
- Before creating a new error type, check if an existing type can handle it.
- Use the `main()` function return value to return the error code.

    ```C++
    int main () {
    	/* ... */
    	catch (...) {
    	    tryLogCurrentException();
    	    return getCurrentExceptionCode();
    	}
    }
    ```

*How to define new exceptions?*
-  A new exception type should only be defined if there is a situation in which code could catch and react specifically to that situation.
-  The exception name should always end with `Exception`.
-  When writing error messages we follow the [Postgres Error Message Style Guide](https://www.postgresql.org/docs/current/error-style-guide.html).


## Alternatives

### Exception Class Hierarchies

Instead of centralizing all exception handling into a single class, we can implement a hierarchy of exception classes. This would allow us to create specific exception types that inherit from a common base class, enabling more granular control over exception handling. For example, we could have a base class `QueryException` with subclasses like `QueryCompilationException` and `QueryRuntimeException`. This approach facilitates the categorization of errors and can make the code readable and maintainable.

However, we did not choose this approach because it can lead to a complex and deeply nested class structure, making it harder to manage and extend. Moreover, ensuring that each new exception type fits correctly into the hierarchy can be difficult, especially when the codebase evolves. A centralized exception handling system with error codes is simpler to maintain and can easily accommodate new error types without the need for extensive modifications to the class hierarchy.

## Appendix

### Current error handling in NebulaStream
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
- Replace all exception classes with the centralized above (`Exception.hpp`).
- Let all main classes in their ´main()´ function return the error codes.
- Adjust current error handling to follow the guidelines.

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

[Postgres Error Style Guide](https://www.postgresql.org/docs/current/error-style-guide.html)

[SQLSTATE Defintion](https://www.ibm.com/docs/de/db2/11.1?topic=messages-sqlstate)

[ClickHouse Error Tables](https://clickhouse.com/docs/en/operations/system-tables/errors)
