#include <Experimental/Interpreter/ExecutionContext.hpp>
#include <Experimental/Interpreter/FunctionCall.hpp>
#include <Experimental/Interpreter/PipelineContext.hpp>
#include <Experimental/Interpreter/RecordBuffer.hpp>
#include <Experimental/Runtime/PipelineContext.hpp>
#include <Runtime/TupleBuffer.hpp>

namespace NES::ExecutionEngine::Experimental::Interpreter {

PipelineContext::PipelineContext(Value<MemRef> pipelineContextRef) : pipelineContextRef(pipelineContextRef) {}

void emitBufferProxy(void* pipelineContext, void* tupleBuffer) {
    auto* pc = (Runtime::Execution::PipelineContext*) pipelineContext;
    auto* tb = (Runtime::TupleBuffer*) tupleBuffer;
    pc->dispatchBuffer(*tb);
    delete tb;
}

void PipelineContext::emitBuffer(const RecordBuffer& rb) {
    FunctionCall<>("emitBuffer", emitBufferProxy, pipelineContextRef, rb.tupleBufferRef);
}

void PipelineContext::registerGlobalOperatorState(const Operator* operatorPtr, std::unique_ptr<OperatorState> operatorState) {
    // this should not be called during trace.
    auto ctx = (Runtime::Execution::PipelineContext*) pipelineContextRef.value->value;
    auto tag = ctx->registerGlobalOperatorState(std::move(operatorState));
    this->operatorIndexMap[operatorPtr] = tag;
}

void* getGlobalOperatorStateProxy(void* pipelineContext, uint64_t tag) {
    auto* pc = (Runtime::Execution::PipelineContext*) pipelineContext;
    return pc->getGlobalOperatorState(tag);
}

Value<MemRef> PipelineContext::getGlobalOperatorState(const Operator* operatorPtr) {
    auto tag = this->operatorIndexMap[operatorPtr];
    // The tag is assumed to be constant therefore we create a constant string and call the get global operator state function with it.
    auto tagValue = Value<Integer>((int64_t) tag);
    return FunctionCall<>("getGlobalOperatorState", getGlobalOperatorStateProxy, pipelineContextRef, tagValue);
}

}// namespace NES::ExecutionEngine::Experimental::Interpreter