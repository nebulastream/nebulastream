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
#ifndef NES_NES_EXECUTION_INCLUDE_INTERPRETER_EXPRESSIONS_READFIELDEXPRESSION_HPP_
#define NES_NES_EXECUTION_INCLUDE_INTERPRETER_EXPRESSIONS_READFIELDEXPRESSION_HPP_

#include <Experimental/Interpreter/DataValue/Value.hpp>
#include <Experimental/Interpreter/Expressions/Expression.hpp>
#include <Experimental/Interpreter/Record.hpp>

namespace NES::Interpreter {

class ReadFieldExpression : public Expression {
  private:
    uint64_t fieldIndex;

  public:
    ReadFieldExpression(uint64_t fieldIndex);
    Value<> execute(Record& record) override;
};

}// namespace NES::Interpreter

#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_EXPRESSIONS_READFIELDEXPRESSION_HPP_
