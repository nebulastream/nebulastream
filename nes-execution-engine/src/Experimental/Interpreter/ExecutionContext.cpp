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
#include <Experimental/Interpreter/RecordBuffer.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/WorkerContext.hpp>

namespace NES::ExecutionEngine::Experimental::Interpreter {

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
    return Value<MemRef>(std::make_unique<MemRef>((int8_t*) res));
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

void ExecutionContext::setGlobalOperatorState(const Operator* op, std::unique_ptr<OperatorState> state) {
    globalStateMap.insert(std::make_pair(op, std::move(state)));
}

OperatorState* ExecutionContext::getLocalState(const Operator* op) {
    auto& value = localStateMap[op];
    return value.get();
}

OperatorState* ExecutionContext::getGlobalState(const Operator* op){
    auto& value = globalStateMap[op];
    return value.get();
}

Value<MemRef> ExecutionContext::allocateBuffer() { return FunctionCall<>("allocateBuffer",allocateBufferProxy, workerContext); }

void ExecutionContext::emitBuffer(const RecordBuffer& rb) { FunctionCall<>("emitBuffer",emitBufferProxy, pipelineContext, rb.tupleBufferRef); }
ExecutionContext::ExecutionContext(Value<MemRef> pipelineContext, Value<MemRef> workerContext)
    : pipelineContext(pipelineContext), workerContext(workerContext) {}

}// namespace NES::ExecutionEngine::Experimental::Interpreter