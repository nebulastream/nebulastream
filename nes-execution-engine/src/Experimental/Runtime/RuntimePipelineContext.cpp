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
#include <Experimental/Interpreter/Operators/Operator.hpp>
#include <Experimental/Runtime/RuntimePipelineContext.hpp>
#include <Runtime/TupleBuffer.hpp>

namespace NES::Runtime::Execution {

void RuntimePipelineContext::dispatchBuffer(Runtime::TupleBuffer&) {}

RuntimePipelineContext::OperatorStateTag RuntimePipelineContext::registerGlobalOperatorState(
    std::unique_ptr<ExecutionEngine::Experimental::Interpreter::OperatorState> operatorState) {
    operatorStates.push_back(std::move(operatorState));
    return operatorStates.size() - 1;
}

ExecutionEngine::Experimental::Interpreter::OperatorState*
RuntimePipelineContext::getGlobalOperatorState(RuntimePipelineContext::OperatorStateTag tag) {
    return operatorStates[tag].get();
}

}// namespace NES::Runtime::Execution