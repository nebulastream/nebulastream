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

#include <Experimental/Interpreter/ExecutionContext.hpp>
#include <Experimental/Interpreter/FunctionCall.hpp>
#include <Experimental/Interpreter/PipelineContext.hpp>
#include <Experimental/Interpreter/RecordBuffer.hpp>
#include <Experimental/Runtime/RuntimePipelineContext.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <cstdint>

namespace NES::ExecutionEngine::Experimental::Interpreter {

PipelineContext::PipelineContext(Value<MemRef> pipelineContextRef) : pipelineContextRef(pipelineContextRef) {}

extern "C" __attribute__((always_inline)) void NES__QueryCompiler__PipelineContext__emitBufferProxy(void* pipelineContext, void* tupleBuffer) {
    auto* pc = (Runtime::Execution::RuntimePipelineContext*) pipelineContext;
    auto* tb = (Runtime::TupleBuffer*) tupleBuffer;
    pc->dispatchBuffer(*tb);
    delete tb;
}

void PipelineContext::emitBuffer(const RecordBuffer& rb) {
    FunctionCall<>("NES__QueryCompiler__PipelineContext__emitBufferProxy", NES__QueryCompiler__PipelineContext__emitBufferProxy, pipelineContextRef, rb.tupleBufferRef);
}

void PipelineContext::registerGlobalOperatorState(const Operator* operatorPtr, std::unique_ptr<OperatorState> operatorState) {
    // this should not be called during trace.
    auto ctx = (Runtime::Execution::RuntimePipelineContext*) pipelineContextRef.value->value;
    auto tag = ctx->registerGlobalOperatorState(std::move(operatorState));
    this->operatorIndexMap[operatorPtr] = tag;
}

extern "C" __attribute__((always_inline)) void* NES__QueryCompiler__PipelineContext__getGlobalOperatorStateProxy(void* pipelineContext, uint64_t tag) {
    auto* pc = (Runtime::Execution::RuntimePipelineContext*) pipelineContext;
    return pc->getGlobalOperatorState(tag);
}


Value<MemRef> PipelineContext::getGlobalOperatorState(const Operator* operatorPtr) {
    auto tag = this->operatorIndexMap[operatorPtr];
    // The tag is assumed to be constant therefore we create a constant string and call the get global operator state function with it.
    auto tagValue = Value<Int32>((int32_t) tag);
    return FunctionCall<>("NES__QueryCompiler__PipelineContext__getGlobalOperatorStateProxy", NES__QueryCompiler__PipelineContext__getGlobalOperatorStateProxy, pipelineContextRef, tagValue);
}

}// namespace NES::ExecutionEngine::Experimental::Interpreter