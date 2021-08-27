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

#ifndef NES_INCLUDE_QUERYCOMPILER_INTERPRETER_OPERATORS_FILTEROPERATOR_HPP_
#define NES_INCLUDE_QUERYCOMPILER_INTERPRETER_OPERATORS_FILTEROPERATOR_HPP_
#include <QueryCompiler/Interpreter/Operators/ExecutableOperator.hpp>
namespace NES::QueryCompilation {
class Expression;
using ExpressionPtr = std::shared_ptr<Expression>;

class FilterOperator : public ExecutableOperator {
  public:
    FilterOperator(ExecutableOperatorPtr next, ExpressionPtr expression);
    void execute(RecordPtr record, ExecutionContextPtr ctx) const override;

  private:
    const ExpressionPtr expression;
};

}// namespace NES::QueryCompilation
#endif//NES_INCLUDE_QUERYCOMPILER_INTERPRETER_OPERATORS_FILTEROPERATOR_HPP_
