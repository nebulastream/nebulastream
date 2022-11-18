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
#include <Experimental/Runtime/RuntimeExecutionContext.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/WorkerContext.hpp>

namespace NES::Nautilus {

void RuntimeExecutionContext::setLocalOperatorState(const Operator* op, std::unique_ptr<OperatorState> state) {
    localStateMap.insert(std::make_pair(op, std::move(state)));
}

void RuntimeExecutionContext::setGlobalOperatorState(const Operator* op, std::unique_ptr<OperatorState> state) {
    globalStateMap.insert(std::make_pair(op, std::move(state)));
}

OperatorState* RuntimeExecutionContext::getLocalState(const Operator* op) {
    auto& value = localStateMap[op];
    return value.get();
}

//OperatorState* ExecutionContext::getGlobalState(const Operator* op){
//    auto& value = globalStateMap[op];
//    return value.get();
//}

extern "C" void* getWorkerContextProxy(void* executionContextPtr) {
    auto executionContext = (Runtime::Execution::RuntimeExecutionContext*) executionContextPtr;
    return executionContext->getWorkerContext();
}

WorkerContext RuntimeExecutionContext::getWorkerContext() {
    auto workerContextRef = FunctionCall<>("getWorkerContext", getWorkerContextProxy, executionContext);
    return WorkerContext(workerContextRef);
}

RuntimeExecutionContext::RuntimeExecutionContext(Value<MemRef> executionContext) : executionContext(executionContext) {}

extern "C" void* getPipelineContextProxy(void* executionContextPtr) {
    auto executionContext = (Runtime::Execution::RuntimeExecutionContext*) executionContextPtr;
    return executionContext->getPipelineContext();
}

PipelineContext RuntimeExecutionContext::getPipelineContext() {
    auto pipelineContextRef = FunctionCall<>("getPipelineContext", getPipelineContextProxy, executionContext);
    return PipelineContext(pipelineContextRef);
}

const Value<MemRef>& RuntimeExecutionContext::getReference() { return this->executionContext; }

}// namespace NES::Nautilus