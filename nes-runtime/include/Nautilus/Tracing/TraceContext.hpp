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

#ifndef NES_NES_RUNTIME_INCLUDE_NAUTILUS_TRACING_TRACECONTEXT_HPP_
#define NES_NES_RUNTIME_INCLUDE_NAUTILUS_TRACING_TRACECONTEXT_HPP_
#include <Nautilus/IR/Types/StampFactory.hpp>
#include <Nautilus/Tracing/SymbolicExecution/SymbolicExecutionContext.hpp>
#include <Nautilus/Tracing/Tag.hpp>
#include <Nautilus/Tracing/TagRecorder.hpp>
#include <Nautilus/Tracing/Trace/ExecutionTrace.hpp>
#include <Nautilus/Tracing/Trace/OpCode.hpp>
#include <Nautilus/Tracing/ValueRef.hpp>
#include <functional>
#include <memory>

namespace NES::Nautilus {
class Any;
typedef std::shared_ptr<Any> AnyPtr;
}// namespace NES::Nautilus

namespace NES::Nautilus::Tracing {

class TraceContext {
  public:
    static TraceContext* get();
    static TraceContext* initialize();
    static void terminate();
    TraceContext();
    void initializeTraceIteration(std::unique_ptr<TagRecorder> tagRecorder);
    void addTraceArgument(const ValueRef& argument);
    void traceConstOperation(const ValueRef& valueReference, const AnyPtr& constValue);
    void traceUnaryOperation(const OpCode& op, const ValueRef& inputRef, const ValueRef& resultRef);
    void traceBinaryOperation(const OpCode& op, const ValueRef& leftRef, const ValueRef& rightRef, const ValueRef& resultRef);
    void traceReturnOperation(const ValueRef& resultRef);
    void traceAssignmentOperation(const ValueRef& targetRef, const ValueRef& sourceRef);
    void traceFunctionCall(const ValueRef& resultRef, const std::vector<Nautilus::Tracing::InputVariant>& arguments);
    void traceFunctionCall(const std::vector<Nautilus::Tracing::InputVariant>& arguments);
    void traceStore(const ValueRef& memRef, const ValueRef& valueRef);
    bool traceCMP(const ValueRef& inputRef);
    ValueRef createNextRef(Nautilus::IR::Types::StampPtr type);
    std::shared_ptr<ExecutionTrace> apply(const std::function<ValueRef()>& function);
    virtual ~TraceContext(){};
    std::shared_ptr<ExecutionTrace> getExecutionTrace() { return executionTrace; }

  private:
    bool isExpectedOperation(const OpCode& op);
    void incrementOperationCounter();
    std::unique_ptr<TagRecorder> tagRecorder;
    std::shared_ptr<ExecutionTrace> executionTrace;
    std::unique_ptr<SymbolicExecutionContext> symbolicExecutionContext;
    uint64_t currentOperationCounter = 0;
};

template<typename Functor>
std::shared_ptr<ExecutionTrace> traceFunction(Functor func) {
    auto ctx = TraceContext::initialize();
    auto result = ctx->apply([&func] {
        func();
        return createNextRef(Nautilus::IR::Types::StampFactory::createVoidStamp());
    });
    TraceContext::terminate();
    return result;
}

template<typename Functor>
std::shared_ptr<ExecutionTrace> traceFunctionWithReturn(const Functor func) {
    auto ctx = TraceContext::initialize();
    auto result = ctx->apply([&func] {
        auto res = func();
        return res.ref;
    });
    TraceContext::terminate();
    return result;
}

}// namespace NES::Nautilus::Tracing

#endif//NES_NES_RUNTIME_INCLUDE_NAUTILUS_TRACING_TRACECONTEXT_HPP_
