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

#ifndef NES_NAUTILUS_INCLUDE_NAUTILUS_TRACING_TRACECONTEXT_HPP_
#define NES_NAUTILUS_INCLUDE_NAUTILUS_TRACING_TRACECONTEXT_HPP_
#include <Nautilus/IR/Types/StampFactory.hpp>
#include <Nautilus/Tracing/SymbolicExecution/SymbolicExecutionContext.hpp>
#include <Nautilus/Tracing/Tag/Tag.hpp>
#include <Nautilus/Tracing/Tag/TagRecorder.hpp>
#include <Nautilus/Tracing/Trace/InputVariant.hpp>
#include <Nautilus/Tracing/Trace/OpCode.hpp>
#include <functional>
#include <memory>

namespace NES::Nautilus {
class Any;
typedef std::shared_ptr<Any> AnyPtr;
}// namespace NES::Nautilus

namespace NES::Nautilus::Tracing {
class OperationRef;
class ExecutionTrace;

/**
 * @brief The trace context manages a thread local instance to record a symbolic execution trace of a given Nautilus function.
 * Tracing will be initialized with ether traceFunction or traceFunctionWithReturn.
 */
class TraceContext {
  public:
    template<typename Functor>
    friend std::shared_ptr<ExecutionTrace> traceFunction(const Functor&& func);
    template<typename Functor>
    friend std::shared_ptr<ExecutionTrace> traceFunctionWithReturn(const Functor&& func);

    /**
     * @brief Get a thread local reference to the trace context.
     * If the trace context is not initialized this function returns a nullptr.
     * @return TraceContext*
     */
    static TraceContext* get();
    static TraceContext* getIfActive();

    static bool shouldTrace();

    /**
     * @brief Add an argument for the traced functioned.
     * @param argument
     */
    void addTraceArgument(const ValueRef& argument);

    /**
     * @brief Trace a constant operation.
     * @param valueReference reference to the const value.
     * @param constValue constant value.
     */
    void traceConstOperation(const ValueRef& valueReference, const AnyPtr& constValue);

    /**
     * @brief Trace a unary operation, e.g., negate.
     * @param op operation code.
     * @param inputRef reference to the input.
     * @param resultRef reference to the result.
     */
    void traceUnaryOperation(const OpCode& op, const ValueRef& inputRef, const ValueRef& resultRef);

    /**
     * @brief Trace a binary operation, e.g., add, sub, div.
     * @param op operation code-
     * @param leftRef reference to the left input.
     * @param rightRef reference to the right input.
     * @param resultRef reference to the result.
     */
    void traceBinaryOperation(const OpCode& op, const ValueRef& leftRef, const ValueRef& rightRef, const ValueRef& resultRef);

    /**
     * @brief Trace the return function.
     * @param resultRef referent to the return value.
     */
    void traceReturnOperation(const ValueRef& resultRef);

    /**
     * @brief Trace a value assignment.
     * @param targetRef reference to the target value.
     * @param sourceRef reference to the source value.
     */
    void traceAssignmentOperation(const ValueRef& targetRef, const ValueRef& sourceRef);

    /**
     * @brief Trace a function call with a result.
     * @param resultRef reference to the function result.
     * @param arguments function arguments.
     */
    void traceFunctionCall(const ValueRef& resultRef, const std::vector<Nautilus::Tracing::InputVariant>& arguments);

    /**
     * @brief Trace a void function call.
     * @param arguments function arguments.
     */
    void traceFunctionCall(const std::vector<Nautilus::Tracing::InputVariant>& arguments);

    /**
     * @brief Trace a memory store.
     * @param memRef reference to memory.
     * @param valueRef reference to the value.
     */
    void traceStore(const ValueRef& memRef, const ValueRef& valueRef);

    /**
     * @brief Trace a cmp operation, for instance in an if or loops.
     * The symbolic executor decides if the cmp will be evaluated to true or false.
     * @param inputRef.
     * @return indicates if the cmp should be taken or not.
     */
    bool traceCMP(const ValueRef& inputRef);
    ValueRef createNextRef(const Nautilus::IR::Types::StampPtr& type);
    virtual ~TraceContext() = default;
    void pause() { active = false; }
    void resume() { active = true; }

  private:
    TraceContext(TagRecorder& tagRecorder);
    static TraceContext* initialize(TagRecorder& tagRecorder);
    static void terminate();
    std::shared_ptr<ExecutionTrace> apply(const std::function<ValueRef()>& function);
    bool isExpectedOperation(const OpCode& op);
    bool isKnownOperation(const Tag* tag);
    std::shared_ptr<OperationRef> checkTag(const Tag* tag);
    void incrementOperationCounter();
    void initializeTraceIteration();
    template<typename Functor>
    void trace(const OpCode& opCode, Functor initFunction);
    bool active = false;
    TagRecorder& tagRecorder;
    std::shared_ptr<ExecutionTrace> executionTrace;
    SymbolicExecutionContext symbolicExecutionContext;
    uint32_t currentOperationCounter = 0;
    std::unordered_map<const Tag*, std::shared_ptr<OperationRef>> tagMap;
    std::unordered_map<const Tag*, std::shared_ptr<OperationRef>> localTagMap;
};

template<typename Functor>
std::shared_ptr<ExecutionTrace> traceFunction(const Functor&& func) {
    auto tr = TagRecorder::createTagRecorder();
    auto ctx = TraceContext::initialize(tr);
    auto result = ctx->apply([&func] {
        func();
        return createNextRef(Nautilus::IR::Types::StampFactory::createVoidStamp());
    });
    TraceContext::terminate();
    return result;
}

template<typename Functor>
std::shared_ptr<ExecutionTrace> traceFunctionWithReturn(const Functor&& func) {
    auto tr = TagRecorder::createTagRecorder();
    auto ctx = TraceContext::initialize(tr);
    auto result = ctx->apply([&func] {
        auto res = func();
        return res.ref;
    });
    TraceContext::terminate();
    return result;
}

}// namespace NES::Nautilus::Tracing

#endif // NES_NAUTILUS_INCLUDE_NAUTILUS_TRACING_TRACECONTEXT_HPP_
