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

#ifndef NES_INCLUDE_OPTIMIZER_UTILS_EXPRESSIONTOZ3EXPRUTIL_HPP_
#define NES_INCLUDE_OPTIMIZER_UTILS_EXPRESSIONTOZ3EXPRUTIL_HPP_

#include <memory>

namespace z3 {
class expr;
using ExprPtr = std::shared_ptr<expr>;

class context;
using ContextPtr = std::shared_ptr<context>;
}// namespace z3

namespace NES {
class ExpressionNode;
using ExpressionNodePtr = std::shared_ptr<ExpressionNode>;
}// namespace NES

namespace NES::Optimizer {

class Z3ExprAndFieldMap;
using Z3ExprAndFieldMapPtr = std::shared_ptr<Z3ExprAndFieldMap>;
/**
 * @brief This class is responsible for taking input as a logical expression and generating an equivalent Z3 expression.
 */
class ExpressionToZ3ExprUtil {

  public:
    /**
     * @brief Convert input expression into an equivalent Z3 expressions
     * @param expression: the input expression
     * @param context: Z3 context
     * @return returns Z3 expression and field map
     */
    static Z3ExprAndFieldMapPtr createForExpression(const ExpressionNodePtr& expression, const z3::ContextPtr& context);

  private:
    /**
     * @brief Convert input Logical expression into an equivalent Z3 expression
     * @param expression: the input logical expression
     * @param context: the Z3 context
     * @return returns Z3 expression and field map
     */
    static Z3ExprAndFieldMapPtr createForLogicalExpressions(const ExpressionNodePtr& expression, const z3::ContextPtr& context);

    /**
     * @brief Convert input arithmetic expression into an equivalent Z3 expression
     * @param expression: the input arithmetic expression
     * @param context: the Z3 context
     * @return returns Z3 expression and field map
     */
    static Z3ExprAndFieldMapPtr createForArithmeticalExpressions(const ExpressionNodePtr& expression,
                                                                 const z3::ContextPtr& context);
};
}// namespace NES::Optimizer

#endif  // NES_INCLUDE_OPTIMIZER_UTILS_EXPRESSIONTOZ3EXPRUTIL_HPP_
