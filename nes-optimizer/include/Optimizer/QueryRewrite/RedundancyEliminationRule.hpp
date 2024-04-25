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

#ifndef NES_OPTIMIZER_INCLUDE_OPTIMIZER_QUERYREWRITE_REDUNDANCYELIMINATIONRULE_HPP_
#define NES_OPTIMIZER_INCLUDE_OPTIMIZER_QUERYREWRITE_REDUNDANCYELIMINATIONRULE_HPP_

#include <Expressions/BinaryExpressionNode.hpp>
#include <Expressions/ExpressionNode.hpp>
#include <Optimizer/QueryRewrite/BaseRewriteRule.hpp>

namespace NES {

class Node;
using NodePtr = std::shared_ptr<Node>;

class LogicalFilterOperator;
using LogicalFilterOperatorPtr = std::shared_ptr<LogicalFilterOperator>;
}// namespace NES

namespace NES::Optimizer {

class RedundancyEliminationRule;
using RedundancyEliminationRulePtr = std::shared_ptr<RedundancyEliminationRule>;

/**
 * @note The rule is not currently used - since the compiler already does most of these optimizations -
 *       but can applied and expanded in the future
 * @brief This class is responsible for reducing redundancies present in the predicates. Namely, three strategies can applied:
 *  1) Constant_moving/folding
 *  Example (folding): filter(Attribute("id") > 5+5) -> filter(Attribute("id") > 10)
 *  Example (moving): filter(Attribute("id") + 10 > 10) -> filter(Attribute("id") > 0)
 *  2) Arithmetic_simplification
 *  Example: filter(Attribute("id") > Attribute("value") * 0) -> filter(Attribute("id") > 0)
 *  3) Conjunction/Disjunction simplification
 *  Example: filter(Attribute("id") > 0 && TRUE) -> filter(Attribute("id") > 0)
 */
class RedundancyEliminationRule : public BaseRewriteRule {

  public:
    QueryPlanPtr apply(QueryPlanPtr queryPlan) override;

    static RedundancyEliminationRulePtr create();
    virtual ~RedundancyEliminationRule() = default;

  private:
    explicit RedundancyEliminationRule();

    /**
     * @brief Remove all possible redundancies by using constant folding, arithmetic simplification and conjunction/disjunction
     * simplification
     * @param predicate
     * @return updated predicate
     */
    static NES::ExpressionNodePtr eliminatePredicateRedundancy(const ExpressionNodePtr& predicate);

    /**
     * @note Currently not implemented
     * @brief Move all constants to the same side of the expression (e.g. a-100 >= 300 becomes a >= 400)
     * @param predicate
     * @return updated predicate
     */
    static NES::ExpressionNodePtr constantMoving(const ExpressionNodePtr& predicate);

    /**
     * @note Already done by the compiler
     * @brief Expressions involving only constants are folded into a single constant (e.g. 2 + 2 becomes 4, 2 = 2 becomes True)
     * @param predicate
     * @return updated predicate
     */
    static NES::ExpressionNodePtr constantFolding(const ExpressionNodePtr& predicate);

    /**
     * @note Already done by the compiler
     * @brief This rule applies arithmetic expressions to which the answer is known (e.g. a * 0 becomes 0, a + 0 becomes a)
     * @param predicate
     * @return updated predicate
     */
    static NES::ExpressionNodePtr arithmeticSimplification(const ExpressionNodePtr& predicate);

    /**
     * @note Currently not implemented, since boolean values in the queries are not supported
     * @brief FALSE in AND operation, the result of the expression is FALSE
     *        TRUE in AND operation, expression can be omitted
     *        FALSE in OR operation, expression can be omitted
     *        TRUE in OR operation, the result of the expression is TRUE
     * @param predicate
     * @return updated predicate
     */
    static NES::ExpressionNodePtr conjunctionDisjunctionSimplification(const ExpressionNodePtr& predicate);
};

}// namespace NES::Optimizer

#endif // NES_OPTIMIZER_INCLUDE_OPTIMIZER_QUERYREWRITE_REDUNDANCYELIMINATIONRULE_HPP_
