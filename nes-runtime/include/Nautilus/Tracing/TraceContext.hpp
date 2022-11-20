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
#ifndef NES_RUNTIME_INCLUDE_NAUTILUS_TRACING_TRACECONTEXT_HPP_
#define NES_RUNTIME_INCLUDE_NAUTILUS_TRACING_TRACECONTEXT_HPP_
#include <Nautilus/Tracing/Tag.hpp>
#include <Nautilus/Tracing/Trace/OpCode.hpp>
#include <Nautilus/Tracing/Trace/TraceOperation.hpp>
#include <functional>
#include <iostream>
#include <list>
#include <memory>
#include <ostream>
#include <unordered_map>
#include <variant>
#include <vector>
namespace NES::Nautilus::Tracing {

class ExecutionTrace;
class TraceOperation;
class OperationRef;
class Tag;

/**
 * @brief Represents the thread local trace context.
 */
class TraceContext {
  public:
    TraceContext();
    void reset();
    void trace(TraceOperation& operation);
    void traceCMP(const ValueRef& valueRef, bool result);
    bool isExpectedOperation(OpCode operation);
    std::shared_ptr<ExecutionTrace> getExecutionTrace();
    ValueRef createNextRef(Nautilus::IR::Types::StampPtr type);
    void addTraceArgument(const ValueRef& value);
    void incrementOperationCounter();

  private:
    TraceOperation& getLastOperation();
    std::shared_ptr<ExecutionTrace> executionTrace;
    uint64_t currentOperationCounter = 0;
    TagAddress startAddress;
};
TraceContext* getThreadLocalTraceContext();
void initThreadLocalTraceContext();
void disableThreadLocalTraceContext();

template<typename Functor>
std::shared_ptr<ExecutionTrace> traceFunction(Functor func) {
    initThreadLocalTraceContext();
    func();
    TraceOperation result = TraceOperation(RETURN);
    auto tracer = getThreadLocalTraceContext();
    tracer->trace(result);
    return tracer->getExecutionTrace();
}

}// namespace NES::Nautilus::Tracing

#endif // NES_RUNTIME_INCLUDE_NAUTILUS_TRACING_TRACECONTEXT_HPP_
