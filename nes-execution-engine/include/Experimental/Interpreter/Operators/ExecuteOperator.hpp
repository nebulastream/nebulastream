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
#ifndef NES_NES_EXECUTION_INCLUDE_INTERPRETER_OPERATORS_EXECUTEOPERATOR_HPP_
#define NES_NES_EXECUTION_INCLUDE_INTERPRETER_OPERATORS_EXECUTEOPERATOR_HPP_
#include <Experimental/Interpreter/Operators/Operator.hpp>
namespace NES::Interpreter {

class Record;

class ExecuteOperator : public Operator {
  public:
    virtual void execute(ExecutionContext& ctx, Record& record) const = 0;
    virtual ~ExecuteOperator() = default;
};

}// namespace NES::Interpreter

#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_OPERATORS_EXECUTEOPERATOR_HPP_
