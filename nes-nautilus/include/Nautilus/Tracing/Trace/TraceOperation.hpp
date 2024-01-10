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

#ifndef NES_NAUTILUS_INCLUDE_NAUTILUS_TRACING_TRACE_TRACEOPERATION_HPP_
#define NES_NAUTILUS_INCLUDE_NAUTILUS_TRACING_TRACE_TRACEOPERATION_HPP_
#include <Nautilus/Tracing/Trace/BlockRef.hpp>
#include <Nautilus/Tracing/Trace/ConstantValue.hpp>
#include <Nautilus/Tracing/Trace/FunctionCallTarget.hpp>
#include <Nautilus/Tracing/Trace/InputVariant.hpp>
#include <Nautilus/Tracing/Trace/OpCode.hpp>
#include <Nautilus/Tracing/ValueRef.hpp>
#include <variant>
#include <vector>
namespace NES::Nautilus::Tracing {

class OperationRef;
/**
 * @brief Represents an individual operation in a trace.
 */
class TraceOperation {
  public:
    TraceOperation(OpCode op);
    TraceOperation(OpCode op, const std::vector<InputVariant>& input);
    TraceOperation(OpCode op, ValueRef result, const std::vector<InputVariant>& input);
    TraceOperation(const TraceOperation& other) = default;
    TraceOperation(const TraceOperation&& other);
    TraceOperation& operator=(const TraceOperation& other);
    TraceOperation& operator=(const TraceOperation&& other);
    ~TraceOperation() = default;
    OpCode op;
    InputVariant result;
    std::vector<InputVariant> input;
    std::shared_ptr<OperationRef> operationRef;
    friend std::ostream& operator<<(std::ostream& os, const TraceOperation& operation);
};

}// namespace NES::Nautilus::Tracing

#endif // NES_NAUTILUS_INCLUDE_NAUTILUS_TRACING_TRACE_TRACEOPERATION_HPP_
