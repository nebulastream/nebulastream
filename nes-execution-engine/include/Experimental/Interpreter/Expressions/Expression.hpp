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
#ifndef NES_NES_EXECUTION_INCLUDE_INTERPRETER_EXPRESSIONS_EXPRESSION_HPP_
#define NES_NES_EXECUTION_INCLUDE_INTERPRETER_EXPRESSIONS_EXPRESSION_HPP_
#include <memory>
#include <Nautilus/Interface/DataTypes/Value.hpp>
namespace NES::ExecutionEngine::Experimental::Interpreter {

class Any;
class Record;
using RecordPtr = std::shared_ptr<Record>;
class Expression;
using ExpressionPtr = std::shared_ptr<Expression>;

class Expression {
  public:
    virtual Value<> execute(Record& record) = 0;
    virtual ~Expression() = default;
};

}// namespace NES

#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_EXPRESSIONS_EXPRESSION_HPP_
