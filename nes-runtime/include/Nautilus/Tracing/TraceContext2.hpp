#ifndef NES_NES_RUNTIME_INCLUDE_NAUTILUS_TRACING_TRACECONTEXT2_HPP_
#define NES_NES_RUNTIME_INCLUDE_NAUTILUS_TRACING_TRACECONTEXT2_HPP_
#include <Nautilus/IR/Types/StampFactory.hpp>
#include <Nautilus/Tracing/SymbolicExecution/SymbolicExecutionContext2.hpp>
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

class TraceContext2 {
  public:
    static TraceContext2* get();
    static TraceContext2* initialize();
    TraceContext2();
    void initializeTraceIteration(std::unique_ptr<TagRecorder> tagRecorder);
    void traceConstOperation(const ValueRef& valueReference, const AnyPtr& constValue);
    void traceUnaryOperation(const OpCode& op, const ValueRef& resultRef, const ValueRef& inputRef);
    void traceBinaryOperation(const OpCode& op, const ValueRef& leftRef, const ValueRef& rightRef, const ValueRef& resultRef);
    void traceReturnOperation(const ValueRef& resultRef);
    void traceAssignmentOperation(const ValueRef& targetRef, const ValueRef& sourceRef);
    bool traceCMP(const ValueRef& inputRef);
    ValueRef createNextRef(Nautilus::IR::Types::StampPtr type);
    std::shared_ptr<ExecutionTrace> apply(const std::function<ValueRef()>& function);
    virtual ~TraceContext2(){};
    std::shared_ptr<ExecutionTrace> getExecutionTrace() { return executionTrace; }

  private:
    bool isExpectedOperation(const OpCode& op);
    void incrementOperationCounter();
    std::unique_ptr<TagRecorder> tagRecorder;
    std::shared_ptr<ExecutionTrace> executionTrace;
    std::unique_ptr<SymbolicExecutionContext2> symbolicExecutionContext;
    uint64_t currentOperationCounter = 0;
};

template<typename Functor>
std::shared_ptr<ExecutionTrace> traceFunction2(Functor func) {
    auto ctx = TraceContext2::initialize();
    return ctx->apply([&func] {
        func();
        return createNextRef(Nautilus::IR::Types::StampFactory::createVoidStamp());
    });
}

template<typename Functor>
std::shared_ptr<ExecutionTrace> traceFunction2WithReturn(const Functor func) {
    auto ctx = TraceContext2::initialize();
    return ctx->apply([&func] {
        auto res = func();
        return res.ref;
    });
}

}// namespace NES::Nautilus::Tracing

#endif//NES_NES_RUNTIME_INCLUDE_NAUTILUS_TRACING_TRACECONTEXT2_HPP_
