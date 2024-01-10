/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#ifndef NES_NAUTILUS_INCLUDE_NAUTILUS_TRACING_TRACEUTIL_HPP_
#define NES_NAUTILUS_INCLUDE_NAUTILUS_TRACING_TRACEUTIL_HPP_
#include <Nautilus/Tracing/Trace/OpCode.hpp>
namespace NES::Nautilus::Tracing {
class ValueRef;
}

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

/**
* @brief Trace a constant operation.
* @param valueReference reference to the const value.
* @param constValue constant value.
*/
void traceConstOperation(const AnyPtr& constValue, const Tracing::ValueRef& valueReference);

/**
* @brief Trace a value assignment.
* @param targetRef reference to the target value.
* @param sourceRef reference to the source value.
*/
void traceAssignmentOperation(const Tracing::ValueRef& targetRef, const Nautilus::Tracing::ValueRef& sourceRef);
bool traceBoolOperation(const AnyPtr& boolValue, const Nautilus::Tracing::ValueRef& sourceRef);
/**
* @brief Trace a binary operation, e.g., add, sub, div.
* @param op operation code-
* @param leftRef reference to the left input.
* @param rightRef reference to the right input.
* @param resultRef reference to the result.
*/
void traceBinaryOperation(const Tracing::OpCode& op,
                          const Tracing::ValueRef& resultRef,
                          const Tracing::ValueRef& leftRef,
                          const Tracing::ValueRef& rightRef);
/**
* @brief Trace a unary operation, e.g., negate.
* @param op operation code.
* @param inputRef reference to the input.
* @param resultRef reference to the result.
*/
void traceUnaryOperation(const Tracing::OpCode& op, const Tracing::ValueRef& resultRef, const Tracing::ValueRef& inputRef);

/**
* @brief Trace a memory store.
* @param memRef reference to memory.
* @param valueRef reference to the value.
*/
void traceStoreOperation(const Tracing::ValueRef& memRef, const Tracing::ValueRef& valueRef);

}// namespace NES::Nautilus::Tracing::TraceUtil

#endif // NES_NAUTILUS_INCLUDE_NAUTILUS_TRACING_TRACEUTIL_HPP_
