#include <Optimizer/QueryRewrite/FilterPushDownRule.hpp>
#include <Nodes/Operators/QueryPlan.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/WindowLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/MapLogicalOperatorNode.hpp>
#include <Nodes/Expressions/FieldAssignmentExpressionNode.hpp>
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <Nodes/Util/Iterators/DepthFirstNodeIterator.hpp>
#include <Nodes/Node.hpp>
#include <queue>

namespace NES {

QueryPlanPtr FilterPushDownRule::apply(QueryPlanPtr queryPlanPtr) {

    NES_INFO("FilterPushDownRule: Get all filter nodes in the graph")
    const auto rootOperator = queryPlanPtr->getRootOperator();
    const auto filterOperators = rootOperator->getNodesByType<FilterLogicalOperatorNode>();

    //    struct less_than_key
    //    {
    //        inline bool operator() (const FilterLogicalOperatorNodePtr struct1, const FilterLogicalOperatorNode struct2)
    //        {
    //            return (struct1->getId() <= struct2.getId());
    //        }
    //    };
    NES_INFO("FilterPushDownRule: Sort all filter nodes in increasing order of the operator id")
    //    std::sort(filterOperators.begin(), filterOperators.end(), less_than_key());

    for (auto filterOperator: filterOperators) {
        pushDownFilter(filterOperator);
    }

    return QueryPlanPtr();
}

RewriteRuleType FilterPushDownRule::getType() {
    return FILTER_PUSH_DOWN;
}

void FilterPushDownRule::pushDownFilter(FilterLogicalOperatorNodePtr filterOperator) {

    //Copy list of children into queue for further processing
    std::vector<NodePtr> children = filterOperator->getChildren();
    std::deque<NodePtr> nodesToProcess(children.begin(), children.end());

    while (!nodesToProcess.empty()) {

        // Remove the first node
        NodePtr node = nodesToProcess.front();
        nodesToProcess.pop_front();

        if (node->instanceOf<SourceLogicalOperatorNode>() || node->instanceOf<WindowLogicalOperatorNode>()
            || node->instanceOf<FilterLogicalOperatorNode>()) {

            if (node != filterOperator) {
                FilterLogicalOperatorNodePtr copyOptr = filterOperator->copy();
                node->insertNodeBetweenParentAndThisNode(copyOptr);
                continue;
            }
        } else if (node->instanceOf<MapLogicalOperatorNode>()) {

            std::string mapFieldName = getFieldNameUsedByMapOperator(node);
            bool predicateFieldManipulated = isFieldUsedInFilterPredicate(filterOperator, mapFieldName);

            if (predicateFieldManipulated) {
                FilterLogicalOperatorNodePtr copyOptr = filterOperator->copy();
                node->insertNodeBetweenParentAndThisNode(copyOptr);
                continue;
            } else {
                std::vector<NodePtr> children = node->getChildren();
                std::copy(children.begin(), children.end(), std::back_inserter(nodesToProcess));
            }
        }
    }

    filterOperator->removeAllParents();
    filterOperator->removeChildren();
}

bool FilterPushDownRule::isFieldUsedInFilterPredicate(FilterLogicalOperatorNodePtr filterOperator,
                                                      const std::string fieldName) const {

    const ExpressionNodePtr filterPredicate = filterOperator->getPredicate();
    DepthFirstNodeIterator depthFirstNodeIterator(filterPredicate);

    for (auto itr = depthFirstNodeIterator.begin(); itr != depthFirstNodeIterator.end(); ++itr) {

        if ((*itr)->instanceOf<FieldAccessExpressionNode>()) {
            const FieldAccessExpressionNodePtr
                accessExpressionNode = (*itr)->as<FieldAccessExpressionNode>();

            if (accessExpressionNode->getFieldName() == fieldName) {
                return true;
            }
        }
    }
    return false;
}

std::string FilterPushDownRule::getFieldNameUsedByMapOperator(NodePtr node) const {
    MapLogicalOperatorNodePtr mapLogicalOperatorNodePtr = node->as<MapLogicalOperatorNode>();
    const FieldAssignmentExpressionNodePtr mapExpression = mapLogicalOperatorNodePtr->getMapExpression();
    const FieldAccessExpressionNodePtr field = mapExpression->getField();
    const std::string mapFieldName = field->getFieldName();
    return mapFieldName;
}

}

