//
// Created by anirudh on 15.03.21.
//

#ifndef NES_WINDOWOPERATORRULES_H
#define NES_WINDOWOPERATORRULES_H


#include <Optimizer/QueryRewrite/BaseRewriteRule.hpp>
#include <Optimizer/Utils/QuerySignatureUtil.hpp>

namespace NES {

class Node;
typedef std::shared_ptr<Node> NodePtr;

class FilterLogicalOperatorNode;
typedef std::shared_ptr<FilterLogicalOperatorNode> FilterLogicalOperatorNodePtr;

class WindowOperatorRules;
typedef std::shared_ptr<WindowOperatorRules> WindowOperatorRulesPtr;

/**
 * @brief This class is responsible for altering the query plan to push down the filter as much as possible.
 * Following are the exceptions:
 *  1.) The Leaf node in the query plan will always be source node. This means the filter can't be push below a source node.
 *  2.) If their exists a map/window operator that manipulates the field used in the predicate of the filter
 *      then, the filter can't be pushed down below the map/window.
 *  3.) If their exists another filter operator next to the target filter operator then it can't be pushed down below that
 *      specific filter operator.
 */
class WindowOperatorRules : public Optimizer::BaseRewriteRule {


  public:
    QueryPlanPtr apply(QueryPlanPtr queryPlan) override;

    static WindowOperatorRulesPtr create();

  private:
    explicit WindowOperatorRules();


    void foldWindowAttributes(WindowOperatorRulesPtr windowOperator);

    std::string getFieldNameUsedByJoinOperator(NodePtr node) const;
    std::string getFieldNameUsedByMapOperator(NodePtr node) const;

    bool isFieldsMatchingInWindowAttributes(WindowLogicalOperatorNodePtr WindowOperator, const std::string fieldName) const;
};
}// namespace NES::Optimizer
#endif//NES_WINDOWOPERATORRULES_H
