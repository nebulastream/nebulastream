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

#ifndef NES_INCLUDE_QUERYCOMPILER_INTERPRETER_OPERATORS_OPERATOR_HPP_
#define NES_INCLUDE_QUERYCOMPILER_INTERPRETER_OPERATORS_OPERATOR_HPP_
#include <memory>

namespace NES::QueryCompilation {
class Record;
using RecordPtr = std::shared_ptr<Record>;
class ExecutionContext;
using ExecutionContextPtr = std::shared_ptr<ExecutionContext>;
class Operator;
using OperatorPtr = std::shared_ptr<Operator>;
class ExecutableOperator;
using ExecutableOperatorPtr = std::shared_ptr<ExecutableOperator>;
class Operator {
  public:
    explicit Operator(ExecutableOperatorPtr next);
    virtual ~Operator() = default;
    virtual void open() const;
    virtual void close() const;

  protected:
    ExecutableOperatorPtr next;
};

}// namespace NES::QueryCompilation

#endif//NES_INCLUDE_QUERYCOMPILER_INTERPRETER_OPERATORS_OPERATOR_HPP_
