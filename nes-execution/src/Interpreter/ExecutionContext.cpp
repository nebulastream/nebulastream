#include <Interpreter/ExecutionContext.hpp>
#include <Interpreter/FunctionCall.hpp>
#include <Interpreter/RecordBuffer.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/WorkerContext.hpp>

namespace NES::Interpreter {

void* allocateBufferProxy(void* workerContext) {
    auto* wc = (Runtime::WorkerContext*) workerContext;
    // we allocate a new tuple buffer on the heap and call retain to prevent the deletion of the reference.
    // todo check with ventura
    auto* tb = new Runtime::TupleBuffer(wc->allocateTupleBuffer());
    return tb;
}

Value<MemRef> allocateBufferProxyWrapper(Value<MemRef>& workerContext) {
    auto workerContextPtr = (void*) workerContext.value->getValue();
    auto res = allocateBufferProxy(workerContextPtr);
    return Value<MemRef>(std::make_unique<MemRef>((int64_t) res));
}

void emitBufferProxy(void* pipelineContext, void* tupleBuffer) {
    auto* pc = (Runtime::Execution::PipelineExecutionContext*) pipelineContext;
    auto* tb = (Runtime::TupleBuffer*) tupleBuffer;
    pc->dispatchBuffer(*tb);
    delete tb;
}

void ExecutionContext::setLocalOperatorState(const Operator* op, std::unique_ptr<OperatorState> state) {
    localStateMap.insert(std::make_pair(op, std::move(state)));
}

OperatorState* ExecutionContext::getLocalState(const Operator* op) {
    auto& value = localStateMap[op];
    return value.get();
}

Value<MemRef> ExecutionContext::allocateBuffer() { return FunctionCall<>(allocateBufferProxy, workerContext); }

void ExecutionContext::emitBuffer(const RecordBuffer& rb) { FunctionCall<>(emitBufferProxy, pipelineContext, rb.tupleBufferRef); }
ExecutionContext::ExecutionContext(Value<MemRef> pipelineContext, Value<MemRef> workerContext)
    : pipelineContext(pipelineContext), workerContext(workerContext) {}

}// namespace NES::Interpreter