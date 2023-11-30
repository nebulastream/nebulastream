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
#ifndef NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_RELATIONAL_MAP_HPP_
#define NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_RELATIONAL_MAP_HPP_
#include <Execution/Expressions/Expression.hpp>
#include <Execution/Operators/ExecutableOperator.hpp>

namespace NES::Runtime::Execution::Operators {

/**
 * @brief Map operator that evaluates a map expression on a input records.
 * Map expressions read record fields, apply transformations, and can set/update fields.
 */
class Map : public ExecutableOperator {
  public:
    /**
     * @brief Creates a map operator with a map expression.
     * @param mapExpression map expression.
     */
    Map(Runtime::Execution::Expressions::ExpressionPtr mapExpression) : mapExpression(mapExpression){};
    void execute(ExecutionContext& ctx, Record& record) const override;

  private:
    const Runtime::Execution::Expressions::ExpressionPtr mapExpression;
};

}// namespace NES::Runtime::Execution::Operators
#endif  // NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_RELATIONAL_MAP_HPP_
