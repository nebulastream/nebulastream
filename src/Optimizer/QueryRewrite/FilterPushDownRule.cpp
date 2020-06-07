#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <Nodes/Expressions/FieldAssignmentExpressionNode.hpp>
#include <Nodes/Node.hpp>
#include <Nodes/Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/MapLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/WindowLogicalOperatorNode.hpp>
#include <Nodes/Util/Iterators/DepthFirstNodeIterator.hpp>
#include <Optimizer/QueryRewrite/FilterPushDownRule.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <bits/stdc++.h>
#include <queue>

namespace NES {

QueryPlanPtr FilterPushDownRule::apply(QueryPlanPtr queryPlanPtr) {

    NES_INFO("FilterPushDownRule: Get all filter nodes in the graph");
    const auto rootOperators = queryPlanPtr->getRootOperators();
    std::vector<FilterLogicalOperatorNodePtr> filterOperators;

    for(auto rootOperator: rootOperators){
        //FIXME: this will result in adding same filter operator twice in the vector
        // remove the duplicate filters from the vector
        auto filters = rootOperator->getNodesByType<FilterLogicalOperatorNode>();
        filterOperators.insert(filterOperators.end(), filters.begin(), filters.end());
    }

    NES_INFO("FilterPushDownRule: Sort all filter nodes in increasing order of the operator id");

    std::sort(filterOperators.begin(), filterOperators.end(), [](FilterLogicalOperatorNodePtr lhs, FilterLogicalOperatorNodePtr rhs) {
        return lhs->getId() < rhs->getId();
    });

    NES_INFO("FilterPushDownRule: Iterate over all the filter operators to push them down in the query plan");
    for (auto filterOperator : filterOperators) {
        pushDownFilter(filterOperator);
    }

    NES_INFO("FilterPushDownRule: Return the updated query plan");
    return queryPlanPtr;
}

void FilterPushDownRule::pushDownFilter(FilterLogicalOperatorNodePtr filterOperator) {

    NES_INFO("FilterPushDownRule: Get children of current filter");
    std::vector<NodePtr> childrenOfFilter = filterOperator->getChildren();
    NES_INFO("FilterPushDownRule: Copy children to the queue for further processing");
    std::deque<NodePtr> nodesToProcess(childrenOfFilter.begin(), childrenOfFilter.end());

    while (!nodesToProcess.empty()) {

        NES_INFO("FilterPushDownRule: Get first operator for processing");
        NodePtr node = nodesToProcess.front();
        nodesToProcess.pop_front();

        if (node->instanceOf<SourceLogicalOperatorNode>() || node->instanceOf<WindowLogicalOperatorNode>()
            || node->instanceOf<FilterLogicalOperatorNode>()) {

            NES_INFO("FilterPushDownRule: Filter can't be pushed below the " + node->toString() + " operator");

            if (node != filterOperator) {

                NES_INFO("FilterPushDownRule: Adding Filter operator between current operator and its parents");
                FilterLogicalOperatorNodePtr copyOptr = filterOperator->makeACopy();
                if (!(copyOptr->removeAndJoinParentAndChildren()
                      && node->insertBetweenThisAndParentNodes(copyOptr))) {

                    NES_ERROR("FilterPushDownRule: Failure in applying filter push down rule");
                    throw std::logic_error("FilterPushDownRule: Failure in applying filter push down rule");
                }
                continue;
            }
        } else if (node->instanceOf<MapLogicalOperatorNode>()) {

            std::string mapFieldName = getFieldNameUsedByMapOperator(node);
            bool predicateFieldManipulated = isFieldUsedInFilterPredicate(filterOperator, mapFieldName);

            if (predicateFieldManipulated) {
                FilterLogicalOperatorNodePtr copyOptr = filterOperator->makeACopy();
                if (!(copyOptr->removeAndJoinParentAndChildren()
                      && node->insertBetweenThisAndParentNodes(copyOptr))) {

                    NES_ERROR("FilterPushDownRule: Failure in applying filter push down rule");
                    throw std::logic_error("FilterPushDownRule: Failure in applying filter push down rule");
                }
                continue;
            } else {
                std::vector<NodePtr> children = node->getChildren();
                std::copy(children.begin(), children.end(), std::back_inserter(nodesToProcess));
            }
        }
    }

    NES_INFO("FilterPushDownRule: Remove all parents can children of the filter operator");
    filterOperator->removeAllParent();
    filterOperator->removeChildren();
}

bool FilterPushDownRule::isFieldUsedInFilterPredicate(FilterLogicalOperatorNodePtr filterOperator,
                                                      const std::string fieldName) const {

    NES_INFO("FilterPushDownRule: Create an iterator for traversing the filter predicates");
    const ExpressionNodePtr filterPredicate = filterOperator->getPredicate();
    DepthFirstNodeIterator depthFirstNodeIterator(filterPredicate);

    for (auto itr = depthFirstNodeIterator.begin(); itr != depthFirstNodeIterator.end(); ++itr) {

        NES_INFO("FilterPushDownRule: Iterate and find the predicate with FieldAccessExpression Node");
        if ((*itr)->instanceOf<FieldAccessExpressionNode>()) {
            const FieldAccessExpressionNodePtr
                accessExpressionNode = (*itr)->as<FieldAccessExpressionNode>();

            NES_INFO("FilterPushDownRule: Check if the input field name is same as the FieldAccessExpression field name");
            if (accessExpressionNode->getFieldName() == fieldName) {
                return true;
            }
        }
    }
    return false;
}

std::string FilterPushDownRule::getFieldNameUsedByMapOperator(NodePtr node) const {
    NES_INFO("FilterPushDownRule: Find the field name used in map operator");
    MapLogicalOperatorNodePtr mapLogicalOperatorNodePtr = node->as<MapLogicalOperatorNode>();
    const FieldAssignmentExpressionNodePtr mapExpression = mapLogicalOperatorNodePtr->getMapExpression();
    const FieldAccessExpressionNodePtr field = mapExpression->getField();
    const std::string mapFieldName = field->getFieldName();
    return mapFieldName;
}

}// namespace NES
