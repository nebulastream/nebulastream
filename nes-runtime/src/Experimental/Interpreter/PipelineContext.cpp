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
#include <Experimental/Interpreter/PipelineContext.hpp>
#include <Experimental/Interpreter/RecordBuffer.hpp>
#include <Experimental/Runtime/RuntimePipelineContext.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <cstdint>

namespace NES::Nautilus {

PipelineContext::PipelineContext(Value<MemRef> pipelineContextRef) : pipelineContextRef(pipelineContextRef) {}

extern "C" __attribute__((always_inline)) void
NES__QueryCompiler__PipelineContext__emitBufferProxy(void* workerContext, void* pipelineContext, void* tupleBuffer) {
    auto* wc = (Runtime::WorkerContext*) workerContext;
    auto* pc = (Runtime::Execution::RuntimePipelineContext*) pipelineContext;
    auto* tb = (Runtime::TupleBuffer*) tupleBuffer;
    pc->dispatchBuffer(*wc, *tb);
    delete tb;
}

void PipelineContext::emitBuffer(const WorkerContext& workerContext, const RecordBuffer& rb) {
    FunctionCall<>("NES__QueryCompiler__PipelineContext__emitBufferProxy",
                   NES__QueryCompiler__PipelineContext__emitBufferProxy,
                   workerContext.getWorkerContextRef(),
                   pipelineContextRef,
                   rb.tupleBufferRef);
}

Runtime::Execution::RuntimePipelineContext::OperatorStateTag
PipelineContext::registerGlobalOperatorState(const Operator* op, std::unique_ptr<OperatorState> operatorState) {
    // this should not be called during trace.
    auto ctx = (Runtime::Execution::RuntimePipelineContext*) pipelineContextRef.value->value;
    auto value = *((int64_t*) op);
    return ctx->registerGlobalOperatorState(value, std::move(operatorState));
}

extern "C" __attribute__((always_inline)) void*
NES__QueryCompiler__PipelineContext__getGlobalOperatorStateProxy(void* pipelineContext, int64_t tag) {
    auto* pc = (Runtime::Execution::RuntimePipelineContext*) pipelineContext;
    return pc->getGlobalOperatorState(tag);
}

Value<MemRef> PipelineContext::getGlobalOperatorState(const Operator* tag) {
    //auto tag = this->operatorIndexMap[operatorPtr];
    // The tag is assumed to be constant therefore we create a constant string and call the get global operator state function with it.
    auto value = *((int64_t*) tag);
    auto tagValue = Value<Int64>(value);
    return FunctionCall<>("NES__QueryCompiler__PipelineContext__getGlobalOperatorStateProxy",
                          NES__QueryCompiler__PipelineContext__getGlobalOperatorStateProxy,
                          pipelineContextRef,
                          tagValue);
}

}// namespace NES::Nautilus