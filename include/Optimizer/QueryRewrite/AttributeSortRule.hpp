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

#ifndef NES_INCLUDE_OPTIMIZER_QUERY_REWRITE_ATTRIBUTE_SORT_RULE_HPP_
#define NES_INCLUDE_OPTIMIZER_QUERY_REWRITE_ATTRIBUTE_SORT_RULE_HPP_

#include <Nodes/Expressions/ExpressionNode.hpp>
#include <Optimizer/QueryRewrite/BaseRewriteRule.hpp>
#include <memory>

namespace NES {
class FieldAccessExpressionNode;
class ConstantValueExpressionNode;
}// namespace NES

namespace NES::Optimizer {

class AttributeSortRule;
using AttributeSortRulePtr = std::shared_ptr<AttributeSortRule>;

/**
 * @brief This rule is only used for evaluating efficiency of query merging using string based signature computation. This rule
 * will alphabetically sort the attributes provided in the Filter, Map, and Project operators. It will however, won't change the
 * LHS and RHS of a relational or assignment operator.
 *
 * Example:
 *
 * For map:
 * 1. map("b" = "c"+"a") => map("b" = "a"+"c")
 * 2. map("b" = "c"+"b"+"a") => map("b" = "a"+"b"+"c")
 * 3. map("b" = (((("c"+"b")+"a")+"d")+"e")) => map("b" = "a"+"a"+"b"+"c")
 * 4. map("b" = "d"+"c"+"b"*"a") => map("b" = "a"*"b"+"c"+"d")
 * 5. map("b" = "d"+"b"+"c"*"a") => map("b" = "a"*"c"+"b"+"d")
 * 6. map("b" = "c"+10+"a"+100) => map("b" = 10+100+"a"+"c")
 *
 *
 * For Filter:
 * 1. filter("c" * "b" > "d" + "a") => filter("a" + "d" < "b" * "c")
 * 2. filter("c" * "b" > "d" + "a" and "a" < "b") => filter("a" < "b" and "b" * "c" > "a" + "d")
 */
class AttributeSortRule : public BaseRewriteRule {

  public:
    static AttributeSortRulePtr create();
    AttributeSortRule() = default;
    virtual ~AttributeSortRule() = default;

    /**
     * @brief Apply Attribute Sort rule on input query plan
     * @param queryPlan: the original query plan
     * @return updated logical query plan
     */
    QueryPlanPtr apply(QueryPlanPtr queryPlan) override;

  private:
    /**
     * @brief Alphabetically sort the attributes in the operator. This method only expects operators of type filter and map.
     * @param logicalOperator: the operator to be sorted
     * @return pointer to the updated expression
     */
    ExpressionNodePtr sortAttributesInExpression(ExpressionNodePtr expression);

    /**
     * @brief Alphabetically sort the attributes within the arithmetic expression
     * @param expression: the input arithmetic expression
     * @return pointer to the updated expression
     */
    ExpressionNodePtr sortAttributesInArithmeticalExpressions(ExpressionNodePtr expression);

    /**
     * @brief Alphabetically sort the attributes within the logical expression
     * @param expression: the input logical expression
     * @return pointer to the updated expression
     */
    ExpressionNodePtr sortAttributesInLogicalExpressions(const ExpressionNodePtr& expression);

    /**
     * @brief fetch all commutative fields of type field access or constant from the relational or arithmetic expression of type
     * ExpressionType. The fetched fields are then sorted alphabetically.
     * @param expression: the expression to be used
     * @return: vector of expression containing commutative field access or constant expression type
     */
    template<class ExpressionType>
    std::vector<ExpressionNodePtr> fetchCommutativeFields(const ExpressionNodePtr& expression) {
        std::vector<ExpressionNodePtr> commutativeFields;
        if (expression->instanceOf<FieldAccessExpressionNode>() || expression->instanceOf<ConstantValueExpressionNode>()) {
            commutativeFields.push_back(expression);
        } else if (expression->template instanceOf<ExpressionType>()) {
            for (const auto& child : expression->getChildren()) {
                auto childCommutativeFields = fetchCommutativeFields<ExpressionType>(child->template as<ExpressionNode>());
                commutativeFields.insert(commutativeFields.end(), childCommutativeFields.begin(), childCommutativeFields.end());
            }
        }
        return commutativeFields;
    }

    /**
     * @brief Replace the original expression within parent expression with updated expression
     * @param parentExpression: the parent expression containing original expression
     * @param originalExpression: the original expression
     * @param updatedExpression: the updated expression
     */
    bool replaceCommutativeExpressions(const ExpressionNodePtr& parentExpression,
                                       const ExpressionNodePtr& originalExpression,
                                       const ExpressionNodePtr& updatedExpression);

    /**
     * @brief Fetch the value of the left most constant expression or the name of the left most field access expression within
     * the input expression. This information is then used for performing global sorting in case of a binary expression.
     * @param expression: the input expression
     * @return the name or value of field or constant expression
     */
    static std::string fetchLeftMostConstantValueOrFieldName(ExpressionNodePtr expression);
};
}// namespace NES::Optimizer
#endif  // NES_INCLUDE_OPTIMIZER_QUERY_REWRITE_ATTRIBUTE_SORT_RULE_HPP_
