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
