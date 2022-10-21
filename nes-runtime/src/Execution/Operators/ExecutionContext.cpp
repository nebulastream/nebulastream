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

#include <Execution/Operators/ExecutionContext.hpp>

namespace NES::Runtime::Execution {

ExecutionContext::ExecutionContext(Value<NES::Nautilus::MemRef> workerContext, Value<NES::Nautilus::MemRef> pipelineContext)
    : workerContext(workerContext), pipelineContext(pipelineContext) {}

Value<MemRef> ExecutionContext::allocateBuffer() { return nullptr; }

void ExecutionContext::emitBuffer(const NES::Runtime::Execution::RecordBuffer&) {}

Value<UInt64> ExecutionContext::getWorkerId() { return 0ul; }

Operators::OperatorState* ExecutionContext::getLocalState(const Operators::Operator*) { return nullptr; }

void ExecutionContext::setLocalOperatorState(const Operators::Operator* , std::unique_ptr<Operators::OperatorState>) {}

void ExecutionContext::setGlobalOperatorState(const Operators::Operator* , std::unique_ptr<Operators::OperatorState>) {}

}// namespace NES::Runtime::Execution