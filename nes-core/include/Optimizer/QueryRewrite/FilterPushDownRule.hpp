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

#ifndef NES_CORE_INCLUDE_OPTIMIZER_QUERYREWRITE_FILTERPUSHDOWNRULE_HPP_
#define NES_CORE_INCLUDE_OPTIMIZER_QUERYREWRITE_FILTERPUSHDOWNRULE_HPP_

#include <Optimizer/QueryRewrite/BaseRewriteRule.hpp>

namespace NES {

class Node;
using NodePtr = std::shared_ptr<Node>;

class FilterLogicalOperatorNode;
using FilterLogicalOperatorNodePtr = std::shared_ptr<FilterLogicalOperatorNode>;

class FieldAccessExpressionNode;
using FieldAccessExpressionNodePtr = std::shared_ptr<FieldAccessExpressionNode>;
}// namespace NES

namespace NES::Optimizer {

class FilterPushDownRule;
using FilterPushDownRulePtr = std::shared_ptr<FilterPushDownRule>;

/**
 * @brief This class is responsible for altering the query plan to push down the filter as much as possible.
 * Following are the exceptions:
 *  1.) The Leaf node in the query plan will always be source node. This means the filter can't be push below a source node.
 *  2.) Every operator below a filter has it's own set of rules that decide if and how the filter can be pushed below that operator
 */
class FilterPushDownRule : public BaseRewriteRule {

  public:
    QueryPlanPtr apply(QueryPlanPtr queryPlan) override;

    static FilterPushDownRulePtr create();
    virtual ~FilterPushDownRule() = default;

    /**
     * @brief Validate if the input field is used in the filter predicate of the operator
     * @param filterOperator : filter operator whose predicate need to be checked
     * @param fieldName :  name of the field to be checked
     * @return true if field use in the filter predicate else false
     */
    static bool isFieldUsedInFilterPredicate(FilterLogicalOperatorNodePtr const& filterOperator, std::string const& fieldName);

  private:
    explicit FilterPushDownRule();

    /**
     * @brief Push down given filter operator as close to the source operator as possible, by calling this method recursively
     * @param filterOperator that is pushed down the queryPlan
     * @param curOperator the operator through which we want to push the filter.
     * @param parOperator the operator that is the parent of curOperator in this queryPlan
     */
    void pushDownFilter(FilterLogicalOperatorNodePtr filterOperator, NodePtr curOperator, NodePtr parOperator);

    /**
     * In case the filter cant be pushed any further this method is called to remove the filter from its original position in the query plan
     * and insert the filter at the new position of the query plan. (only if the position of the filter changed)
     * @param filterOperator the filter operator that we want to insert into the query plan
     * @param childOperator insert the filter operator above this operator in the query plan
     * @param parOperator  insert the filter operator below this operator in the query plan
     */
    static void
    insertFilterIntoNewPosition(FilterLogicalOperatorNodePtr filterOperator, NodePtr childOperator, NodePtr parOperator);

    /**
     * @brief pushes a filter that is above a join, below that join if that is possible. We differentiate four cases:
     * Starting Position of the query plan:
     *            Filter
     *              |
     *              Join
     *      |               |
     *  left branch     right branch
     *
     *  ==> case 1: All attributes that the filter accesses come from the left branch of the join
     *              Join
     *      |               |
     *    Filter        right branch
     *      |
     *   left branch
     *
     *  ==> case 2: All attributes that the filter accesses come from the right branch of the join
     *             Join
     *      |               |
     *  left branch       Filter
     *                      |
     *                  right branch
     *
     *  ==> case 3: Some attributes that the filter accesses come from different branches of the join
     *  queryPlan stays the same
     *
     *  ==> case 4: All attributes that the filter accesses are part of the join condition
     *  special case described in class method pushFilterBelowJoinSpecialCase()
     *
     * @param filterOperator the filter operator that we try to push down
     * @param joinOperator the join operator to which we want to push the filter down below. (it is currently the child of the filter)
     * @param parentOperator the parent operator of the joinOperator. In case we can not push down the filter, we insert it between
     * joinOperator and parOperator.
     */
    void pushFilterBelowJoin(FilterLogicalOperatorNodePtr filterOperator, NodePtr joinOperator, NodePtr parentOperator);

    /**
     * @brief pushes a filter that is above a join two both branches of the join if that is possible. This only considers Equi-Joins
     * If the filter only accesses one attribute and that attribute is part of the join condition, a duplicate filter can be created and pushed down as well.
     * The original filter will belong to one branch where it is pushed down and the duplicate filter will be pushed down the other branch.
     * The accessed attribute of the duplicate filter will be changed to attribute that is part of the joinCondition from this branch.
     * This is possible because values that are not equal to the values of the original attribute of the filter would get ignored
     * in the join anyways.
     * Example:
     *                  Filter(Src1.a==10)                                                                Join(Src1.a == Src2.a)
     *                          |                                                                       |                       |
     *                  Join(Src1.a == Src2.a)                              ====>             Filter(Src1.a==10)          Filter(Src2.a==10)
     *                  |                     |                                                         |                       |
     *    left branch(contains Src1)        right branch(Contains Src2)                   left branch(contains Src1)        right branch(Contains Src2)
     *
     * @param filterOperator the filter operator that is tried to be pushed
     * @param joinOperator the join operator to which the filter should be tried to be pushed down below. (it is currently the child of the filter)
     * @return true if we pushed the filter to both branches of this joinOperator
     */
    bool pushFilterBelowJoinSpecialCase(FilterLogicalOperatorNodePtr filterOperator, NodePtr joinOperator);

    /**
     * @brief tries to push the filter below a map operator. This is possible as long as the map operator doesn't change any attribute that the filter uses
     * @param filterOperator the filter operator that we try to push down
     * @param mapOperator the map operator to which we want to push the filter down below. (it is currently the child of the filter)
     * mapOperator and parOperator
     */
    void pushFilterBelowMap(FilterLogicalOperatorNodePtr filterOperator, NodePtr mapOperator);

    /**
     * @brief pushes the filter below a union operator to both branches of the union. Both branches of the union have the same Attributes,
     * so we can always push the filter to both sides.
     * @param filterOperator the filter operator to be pushed down
     * @param unionOperator the union operator to which the filter should be pushed down below. (it is currently the child of the filter)
     */
    void pushFilterBelowUnion(FilterLogicalOperatorNodePtr filterOperator, NodePtr unionOperator);

    /**
     * @brief pushes the filter below a keyed window operator. This is only possible if all the attributes accessed by the filter are
     * not aggregated themselves but are a part of the groupBy clause. In this case no aggregation will be effected but it and the
     * corresponding groups would be filtered out afterwards anyways.
     * Example:
     *          Filter(a==5 || b==10)                                   avg().groupBy(a,b,c)
     *                      |                           ====>                   |
     *               avg().groubBy(a,b,c)                               Filter(a==5, b==10)
     *
     * @param filterOperator the filter operator to be pushed down
     * @param windowOperator the window operator to which the filter should be pushed down below. (it is currently the child of the filter)
     * @param parOperator the parent operator of the windowOperator. In case the filter cant be pushed down, it is inserted between
     * windowOperator and parOperator
     */
    void
    pushFilterBelowWindowAggregation(FilterLogicalOperatorNodePtr filterOperator, NodePtr windowOperator, NodePtr parOperator);

    /**
     * @brief Get the name of the field manipulated by the Map operator
     * @param node the map operator node
     * @return name of the field
     */
    static std::string getFieldNameUsedByMapOperator(const NodePtr& node);

    /**
     * @brief Get the @link FieldAccessExpressionNodePtr @endlink used in the filter predicate
     * @param filterOperator
     * @return @link std::vector<FieldAccessExpressionNodePtr> @endLink
     */
    static std::vector<FieldAccessExpressionNodePtr> getFilterAccessExpressions(const ExpressionNodePtr& filterPredicate);

    /**
     * @brief pushes a filter below a projection operator. If the projection renames a attribute that is used by the filter,
     * the filter attribute name is renamed.
     *
     * Sink                             Sink
     * |                                |
     * Filter(b = 10)                   Projection(b, a -> b)
     * |                                |
     * Projection(a, a -> b)            Filter(a = 10)
     * |                                |
     * Src(a, b)                        Src(a, b)
     *
     * @param filterOperator the filter operator to be pushed down
     * @param projectionOperator the projection operator to which the filter should be pushed down below. (it is currently the child of the filter)
     * @return @link std::vector<FieldAccessExpressionNodePtr> @endLink
     */
    void pushBelowProjection(FilterLogicalOperatorNodePtr filterOperator, NodePtr projectionOperator);

    /**
     * @brief Rename the attributes in the filter predicate if the attribute is changed by the expression node
     * @param filterOperator filter operator whose predicate need to be checked and updated
     * @param expressionNodes expression nodes containing the attribute name and the new attribute name
     */
    static void renameFilterAttributesByExpressionNodes(const FilterLogicalOperatorNodePtr& filterOperator,
                                                        const std::vector<ExpressionNodePtr>& expressionNodes);

    /**
     * @brief Rename the attribute in the field access expression node.
     * @param expressionNode to be renamed
     * @param toReplace attribute name to be replaced
     * @param replacement new attribute name
     */
    static void
    renameFieldAccessExpressionNodes(ExpressionNodePtr expressionNode, std::string toReplace, std::string replacement);

    void substituteFilterAttributeWithMapTransformation(FilterLogicalOperatorNodePtr const& filterOperator,
                                                                            const MapLogicalOperatorNodePtr& mapOperator, const std::string& fieldName)
};

}// namespace NES::Optimizer

#endif// NES_CORE_INCLUDE_OPTIMIZER_QUERYREWRITE_FILTERPUSHDOWNRULE_HPP_
