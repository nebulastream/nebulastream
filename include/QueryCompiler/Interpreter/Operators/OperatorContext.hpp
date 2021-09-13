/*

     Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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
#ifndef NES_INCLUDE_QUERYCOMPILER_INTERPRETER_OPERATORS_OPERATORCONTEXT_HPP_
#define NES_INCLUDE_QUERYCOMPILER_INTERPRETER_OPERATORS_OPERATORCONTEXT_HPP_
#include <Runtime/BufferManager.hpp>
#include <QueryCompiler/Interpreter/ForwardDeclaration.hpp>
#include <map>
#include <memory>
namespace NES::Runtime {
class TupleBuffer;
}
namespace NES::QueryCompilation {

class Record;
using RecordPtr = std::shared_ptr<Record>;
class Operator;
using OperatorPtr = std::shared_ptr<const Operator>;

class OperatorState {
  public:
    OperatorState() = default;
    virtual ~OperatorState() = default;
};
using OperatorStatePtr = std::shared_ptr<OperatorState>;

class ExecutionContext {
  public:
    ExecutionContext(Runtime::BufferManagerPtr bufferManager);
    Runtime::TupleBuffer allocateTupleBuffer();
    auto setThreadLocalOperator(OperatorPtr op, OperatorStatePtr operatorState) { threadLocalState[op] = operatorState; }
    template<class OperatorStateType>
    auto getThreadLocalOperator(OperatorPtr op) {
        return std::dynamic_pointer_cast<OperatorStateType>(threadLocalState[op]);
    }

  private:
    Runtime::BufferManagerPtr bufferManager;
    std::map<OperatorPtr, OperatorStatePtr> threadLocalState;
};

}// namespace NES::QueryCompilation

#endif//NES_INCLUDE_QUERYCOMPILER_INTERPRETER_OPERATORS_OPERATORCONTEXT_HPP_
