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
#include <Experimental/Runtime/ExecutionContext.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/WorkerContext.hpp>

namespace NES::ExecutionEngine::Experimental::Interpreter {

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

//OperatorState* ExecutionContext::getGlobalState(const Operator* op){
//    auto& value = globalStateMap[op];
//    return value.get();
//}

void* getWorkerContextProxy(void* executionContextPtr) {
    auto executionContext = (Runtime::Execution::ExecutionContext*) executionContextPtr;
    return executionContext->getWorkerContext();
}

WorkerContext ExecutionContext::getWorkerContext() {
    auto workerContextRef = FunctionCall<>("getWorkerContext", getWorkerContextProxy, executionContext);
    return WorkerContext(workerContextRef);
}

ExecutionContext::ExecutionContext(Value<MemRef> executionContext) : executionContext(executionContext) {}

void* getPipelineContextProxy(void* executionContextPtr) {
    auto executionContext = (Runtime::Execution::ExecutionContext*) executionContextPtr;
    return executionContext->getPipelineContext();
}

PipelineContext ExecutionContext::getPipelineContext() {
    auto pipelineContextRef = FunctionCall<>("getPipelineContext", getPipelineContextProxy, executionContext);
    return PipelineContext(pipelineContextRef);
}

}// namespace NES::ExecutionEngine::Experimental::Interpreter