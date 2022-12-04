#ifndef NES_NES_RUNTIME_INCLUDE_NAUTILUS_TRACING_TRACEUTIL_HPP_
#define NES_NES_RUNTIME_INCLUDE_NAUTILUS_TRACING_TRACEUTIL_HPP_

namespace NES::Nautilus::Tracing::TraceUtil {

/**
 * Returns a boolean value indicating whether the method is executed in the interpreter.
 * @return {@code true} when executed in the interpreter, {@code false} in compiled code.
 */
[[maybe_unused, nodiscard]] bool inInterpreter();

/**
 * Returns a boolean value indicating whether the method is executed in the tracer.
 * @return {@code true} when executed in the tracer, {@code false} in compiled code.
 */
[[maybe_unused, nodiscard]] bool inTracer();

}// namespace NES::Nautilus::Tracing::TraceUtil

#endif//NES_NES_RUNTIME_INCLUDE_NAUTILUS_TRACING_TRACEUTIL_HPP_
