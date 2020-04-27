#ifndef NES_IMPL_OPTIMIZER_QUERYREWRITE_FILTERPUSHDOWN_HPP_
#define NES_IMPL_OPTIMIZER_QUERYREWRITE_FILTERPUSHDOWN_HPP_

#include <Optimizer/QueryRewrite/BaseRewriteRule.hpp>

namespace NES {

class Node;
typedef std::shared_ptr<Node> NodePtr;

class FilterLogicalOperatorNode;
typedef std::shared_ptr<FilterLogicalOperatorNode> FilterLogicalOperatorNodePtr;

/**
 * @brief This class is responsible for altering the query plan to push down the filter as much as possible.
 * Following are the exceptions:
 *  1.) The Leaf node in the query plan will always be source node. This means the filter can't be push below a source node.
 *  2.) If their exists a map/window operator that manipulates the field used in the predicate of the filter
 *      then, the filter can't be pushed down below the map/window/.
 *  3.) If their exists another filter operator next to the target filter operator then it can't be pushed down below that
 *      specific filter operator.
 */
class FilterPushDownRule : public BaseRewriteRule {

  public:
    QueryPlanPtr apply(QueryPlanPtr queryPlanPtr) override;
    RewriteRuleType getType() override;

  private:

    /**
     * @brief //FIXME: Define once impl done
     * @param filterOperator
     */
    void pushDownFilter(FilterLogicalOperatorNodePtr filterOperator);

    /**
     * @brief Get the name of the field manipulated by the Map operator
     * @param Operator pointer
     * @return name of the field
     */
    std::string getFieldNameUsedByMapOperator(NodePtr node) const;

    /**
     * @brief Validate if the input field is used in the filter predicate of the operator
     * @param filterOperator : filter operator whose predicate need to be checked
     * @param fieldName :  name of the field to be checked
     * @return true if field use in the filter predicate else false
     */
    bool isFieldUsedInFilterPredicate(FilterLogicalOperatorNodePtr filterOperator, const std::string fieldName) const;
};

}

#endif //NES_IMPL_OPTIMIZER_QUERYREWRITE_FILTERPUSHDOWN_HPP_
