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
#ifndef NES_NES_EXECUTION_INCLUDE_INTERPRETER_EXECUTIONCONTEXT_HPP_
#define NES_NES_EXECUTION_INCLUDE_INTERPRETER_EXECUTIONCONTEXT_HPP_
#include <Execution/Operators/OperatorState.hpp>
#include <Nautilus/Interface/DataTypes/MemRef.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>
#include <memory>
#include <unordered_map>

namespace NES::Runtime::Execution {
using namespace Nautilus;
class RecordBuffer;

namespace Operators {
class Operator;
class OperatorState;
}// namespace Operators

class ExecutionContext final {
  public:
    ExecutionContext(Value<MemRef> workerContext, Value<MemRef> pipelineContext);
    void setLocalOperatorState(const Operators::Operator* op, std::unique_ptr<Operators::OperatorState> state);
    void setGlobalOperatorState(const Operators::Operator* op, std::unique_ptr<Operators::OperatorState> state);
    Operators::OperatorState* getLocalState(const Operators::Operator* op);
    Value<UInt64> getWorkerId();
    Value<MemRef> allocateBuffer();
    void emitBuffer(const RecordBuffer& rb);

  private:
    std::unordered_map<const Operators::Operator*, std::unique_ptr<Operators::OperatorState>> localStateMap;
    std::unordered_map<const Operators::Operator*, std::unique_ptr<Operators::OperatorState>> globalStateMap;
    Value<MemRef> workerContext;
    Value<MemRef> pipelineContext;
};

}// namespace NES::Runtime::Execution

#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_EXECUTIONCONTEXT_HPP_
