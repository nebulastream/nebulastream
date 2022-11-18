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
#include "Experimental/ExecutionEngine/ExecutablePipeline.hpp"
#include <Experimental/Interpreter/ExecutionContext.hpp>
#include <Experimental/Interpreter/Operators/Operator.hpp>
#include <Experimental/Runtime/RuntimePipelineContext.hpp>
#include <Runtime/TupleBuffer.hpp>

namespace NES::Runtime::Execution {

void RuntimePipelineContext::dispatchBuffer(Runtime::WorkerContext& workerContext, Runtime::TupleBuffer& buffer) {
    if (!successors.empty())
        successors[0]->execute(workerContext, buffer);
}

RuntimePipelineContext::OperatorStateTag
RuntimePipelineContext::registerGlobalOperatorState(int64_t op, std::unique_ptr<Nautilus::OperatorState> operatorState) {
    operatorStates.insert(std::make_pair(op, std::move(operatorState)));
    return operatorStates.size() - 1;
}

Nautilus::OperatorState* RuntimePipelineContext::getGlobalOperatorState(int64_t tag) { return operatorStates[tag].get(); }

void RuntimePipelineContext::addSuccessorPipeline(std::shared_ptr<ExecutionEngine::Experimental::ExecutablePipeline> pipeline) {
    successors.emplace_back(pipeline);
}

}// namespace NES::Runtime::Execution