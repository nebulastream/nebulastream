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
#include <Nautilus/Interface/DataTypes/MemRef.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>
#include <Experimental/Interpreter/WorkerContext.hpp>
#include <Experimental/Interpreter/PipelineContext.hpp>
#include <memory>
#include <unordered_map>

namespace NES::Runtime {
class WorkerContext;
}
namespace NES::Nautilus {
class Operator;
class RecordBuffer;

class RuntimeExecutionContext {
  public:
    RuntimeExecutionContext(Value<MemRef> executionContext);
    void setLocalOperatorState(const Operator* op, std::unique_ptr<OperatorState> state);
    void setGlobalOperatorState(const Operator* op, std::unique_ptr<OperatorState> state);
    OperatorState* getLocalState(const Operator* op);
    WorkerContext getWorkerContext();
    PipelineContext getPipelineContext();
    const Value<MemRef>& getReference();

  private:
    std::unordered_map<const Operator*, std::unique_ptr<OperatorState>> localStateMap;
    std::unordered_map<const Operator*, std::unique_ptr<OperatorState>> globalStateMap;
    Value<MemRef> executionContext;
};

}// namespace NES::Nautilus

#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_EXECUTIONCONTEXT_HPP_
