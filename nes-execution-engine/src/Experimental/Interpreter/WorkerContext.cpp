#include <Experimental/Interpreter/FunctionCall.hpp>
#include <Experimental/Interpreter/WorkerContext.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/WorkerContext.hpp>

namespace NES::ExecutionEngine::Experimental::Interpreter {

WorkerContext::WorkerContext(Value<MemRef> workerContextRef): workerContextRef(workerContextRef) {}

void* allocateBufferProxy(void* workerContext) {
    auto* wc = (Runtime::WorkerContext*) workerContext;
    // we allocate a new tuple buffer on the heap and call retain to prevent the deletion of the reference.
    // todo check with ventura
    auto* tb = new Runtime::TupleBuffer(wc->allocateTupleBuffer());
    return tb;
}

Value<MemRef> WorkerContext::allocateBuffer() { return FunctionCall<>("allocateBuffer", allocateBufferProxy, workerContextRef); }

uint64_t getWorkerIdProxy(void* workerContext) {
    auto* wc = (Runtime::WorkerContext*) workerContext;
    return wc->getId();
}

Value<Integer> WorkerContext::getWorkerId() { return FunctionCall<>("getWorkerId", getWorkerIdProxy, workerContextRef); }

}// namespace NES::ExecutionEngine::Experimental::Interpreter