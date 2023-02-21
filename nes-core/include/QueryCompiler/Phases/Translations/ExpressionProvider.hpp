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

#ifndef NES_NES_CORE_INCLUDE_QUERYCOMPILER_PHASES_TRANSLATIONS_LOWEREXPRESSIONS_HPP_
#define NES_NES_CORE_INCLUDE_QUERYCOMPILER_PHASES_TRANSLATIONS_LOWEREXPRESSIONS_HPP_
#include <memory>
namespace NES {
class ExpressionNode;
using ExpressionNodePtr = std::shared_ptr<ExpressionNode>;
class FunctionExpression;

namespace Runtime::Execution::Expressions {
class Expression;
}

namespace QueryCompilation {
class ExpressionProvider {
  public:
    std::shared_ptr<Runtime::Execution::Expressions::Expression> lowerExpression(const ExpressionNodePtr& expressionNode);
  private:
    std::shared_ptr<Runtime::Execution::Expressions::Expression> lowerFunctionExpression(const std::shared_ptr<FunctionExpression>& expressionNode);
};

}// namespace QueryCompilation
}// namespace NES
#endif//NES_NES_CORE_INCLUDE_QUERYCOMPILER_PHASES_TRANSLATIONS_LOWEREXPRESSIONS_HPP_
