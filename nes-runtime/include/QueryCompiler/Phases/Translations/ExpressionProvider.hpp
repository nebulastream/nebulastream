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

#ifndef NES_RUNTIME_INCLUDE_QUERYCOMPILER_PHASES_TRANSLATIONS_EXPRESSIONPROVIDER_HPP_
#define NES_RUNTIME_INCLUDE_QUERYCOMPILER_PHASES_TRANSLATIONS_EXPRESSIONPROVIDER_HPP_
#include <memory>
namespace NES {
class ExpressionNode;
using ExpressionNodePtr = std::shared_ptr<ExpressionNode>;
class FunctionExpression;

namespace Runtime::Execution::Expressions {
class Expression;
using ExpressionPtr = std::shared_ptr<Expression>;
}// namespace Runtime::Execution::Expressions

namespace QueryCompilation {
class ExpressionProvider {
  public:
    Runtime::Execution::Expressions::ExpressionPtr lowerExpression(const ExpressionNodePtr& expressionNode);

  private:
    Runtime::Execution::Expressions::ExpressionPtr
    lowerFunctionExpression(const std::shared_ptr<FunctionExpression>& expressionNode);
    Runtime::Execution::Expressions::ExpressionPtr
    lowerConstantExpression(const std::shared_ptr<ConstantValueExpressionNode>& expressionNode);
};

}// namespace QueryCompilation
}// namespace NES
#endif  // NES_RUNTIME_INCLUDE_QUERYCOMPILER_PHASES_TRANSLATIONS_EXPRESSIONPROVIDER_HPP_
