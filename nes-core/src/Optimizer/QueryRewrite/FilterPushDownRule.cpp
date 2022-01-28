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

#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <Nodes/Expressions/FieldAssignmentExpressionNode.hpp>
#include <Nodes/Node.hpp>
#include <Nodes/Util/Iterators/DepthFirstNodeIterator.hpp>
#include <Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/JoinLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/MapLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/ProjectionLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/UnionLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Windowing/WindowLogicalOperatorNode.hpp>
#include <Optimizer/QueryRewrite/FilterPushDownRule.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <queue>

namespace NES::Optimizer {

FilterPushDownRulePtr FilterPushDownRule::create() { return std::make_shared<FilterPushDownRule>(FilterPushDownRule()); }

FilterPushDownRule::FilterPushDownRule() = default;

QueryPlanPtr FilterPushDownRule::apply(QueryPlanPtr queryPlan) {

    NES_INFO("Applying FilterPushDownRule to query " << queryPlan->toString());
    const auto rootOperators = queryPlan->getRootOperators();
    std::vector<FilterLogicalOperatorNodePtr> filterOperators;
    for (const auto& rootOperator : rootOperators) {
        //FIXME: this will result in adding same filter operator twice in the vector
        // remove the duplicate filters from the vector
        auto filters = rootOperator->getNodesByType<FilterLogicalOperatorNode>();
        filterOperators.insert(filterOperators.end(), filters.begin(), filters.end());
    }
    NES_DEBUG("FilterPushDownRule: Sort all filter nodes in increasing order of the operator id");
    std::sort(filterOperators.begin(),
              filterOperators.end(),
              [](const FilterLogicalOperatorNodePtr& lhs, const FilterLogicalOperatorNodePtr& rhs) {
                  return lhs->getId() < rhs->getId();
              });
    NES_DEBUG("FilterPushDownRule: Iterate over all the filter operators to push them down in the query plan");
    for (const auto& filterOperator : filterOperators) {
        pushDownFilter(filterOperator);
    }
    NES_INFO("FilterPushDownRule: Return the updated query plan " << queryPlan->toString());
    return queryPlan;
}

void FilterPushDownRule::pushDownFilter(const FilterLogicalOperatorNodePtr& filterOperator) {

    NES_TRACE("FilterPushDownRule: Get children of current filter");
    std::vector<NodePtr> childrenOfFilter = filterOperator->getChildren();
    NES_TRACE("FilterPushDownRule: Copy children to the queue for further processing");
    std::deque<NodePtr> nodesToProcess(childrenOfFilter.begin(), childrenOfFilter.end());

    while (!nodesToProcess.empty()) {

        static bool isFilterAboveUnionOperator{false};
        NES_TRACE("FilterPushDownRule: Get first operator for processing");
        NodePtr node = nodesToProcess.front();
        nodesToProcess.pop_front();
        if (node->instanceOf<SourceLogicalOperatorNode>() || node->instanceOf<WindowLogicalOperatorNode>()
            || node->instanceOf<FilterLogicalOperatorNode>() || node->instanceOf<ProjectionLogicalOperatorNode>()
            || node->instanceOf<JoinLogicalOperatorNode>()) {

            NES_TRACE("FilterPushDownRule: Filter can't be pushed below the " + node->toString() + " operator");
            if (node->as<OperatorNode>()->getId() != filterOperator->getId()) {
                NES_TRACE("FilterPushDownRule: Adding Filter operator between current operator and its parents");
                if (isFilterAboveUnionOperator) {//If  filter was above a union operator
                    NES_TRACE("FilterPushDownRule: Create a duplicate filter operator with new operator ID");
                    //Create duplicate of the filter
                    OperatorNodePtr duplicatedFilterOperator = filterOperator->copy();
                    duplicatedFilterOperator->setId(Util::getNextOperatorId());
                    //Inset it between currently traversed node and its parent
                    if (!node->insertBetweenThisAndParentNodes(duplicatedFilterOperator)) {
                        NES_ERROR("FilterPushDownRule: Failure in applying filter push down rule");
                        throw std::logic_error("FilterPushDownRule: Failure in applying filter push down rule");
                    }
                    isFilterAboveUnionOperator = !nodesToProcess.empty();
                } else if (!(filterOperator->removeAndJoinParentAndChildren()
                             && node->insertBetweenThisAndParentNodes(filterOperator->copy()))) {

                    NES_ERROR("FilterPushDownRule: Failure in applying filter push down rule");
                    throw std::logic_error("FilterPushDownRule: Failure in applying filter push down rule");
                }
                continue;
            }
        } else if (node->instanceOf<MapLogicalOperatorNode>()) {

            std::string mapFieldName = getFieldNameUsedByMapOperator(node);
            bool predicateFieldManipulated = isFieldUsedInFilterPredicate(filterOperator, mapFieldName);
            if (predicateFieldManipulated) {
                NES_TRACE("FilterPushDownRule: Adding Filter operator between current operator and its parents");
                if (isFilterAboveUnionOperator) {//If  filter was above a union operator
                    NES_TRACE("FilterPushDownRule: Create a duplicate filter operator with new operator ID");
                    //Create duplicate of the filter
                    OperatorNodePtr duplicatedFilterOperator = filterOperator->copy();
                    duplicatedFilterOperator->setId(Util::getNextOperatorId());
                    //Inset it between currently traversed node and its parent
                    if (!node->insertBetweenThisAndParentNodes(duplicatedFilterOperator)) {
                        NES_ERROR("FilterPushDownRule: Failure in applying filter push down rule");
                        throw std::logic_error("FilterPushDownRule: Failure in applying filter push down rule");
                    }
                    isFilterAboveUnionOperator = !nodesToProcess.empty();
                } else if (!(filterOperator->removeAndJoinParentAndChildren()
                             && node->insertBetweenThisAndParentNodes(filterOperator->copy()))) {
                    NES_ERROR("FilterPushDownRule: Failure in applying filter push down rule");
                    throw std::logic_error("FilterPushDownRule: Failure in applying filter push down rule");
                }
                continue;
            }
            std::vector<NodePtr> children = node->getChildren();
            if (isFilterAboveUnionOperator) {//To ensure duplicated filter operator with a new operator ID consistently moves to sub-query
                std::copy(children.begin(), children.end(), std::front_inserter(nodesToProcess));
            } else {
                std::copy(children.begin(), children.end(), std::back_inserter(nodesToProcess));
            }

        } else if (node->instanceOf<UnionLogicalOperatorNode>()) {
            isFilterAboveUnionOperator = true;
            std::vector<NodePtr> childrenOfMergeOP = node->getChildren();
            std::copy(childrenOfMergeOP.begin(), childrenOfMergeOP.end(), std::front_inserter(nodesToProcess));
            std::sort(nodesToProcess.begin(),
                      nodesToProcess.end());//To ensure consistency in nodes traversed below a merge operator
        }
    }
    filterOperator->removeAndJoinParentAndChildren();
}

bool FilterPushDownRule::isFieldUsedInFilterPredicate(FilterLogicalOperatorNodePtr const& filterOperator,
                                                      const std::string& fieldName) {

    NES_TRACE("FilterPushDownRule: Create an iterator for traversing the filter predicates");
    const ExpressionNodePtr filterPredicate = filterOperator->getPredicate();
    DepthFirstNodeIterator depthFirstNodeIterator(filterPredicate);
    for (auto itr = depthFirstNodeIterator.begin(); itr != NES::DepthFirstNodeIterator::end(); ++itr) {
        NES_TRACE("FilterPushDownRule: Iterate and find the predicate with FieldAccessExpression Node");
        if ((*itr)->instanceOf<FieldAccessExpressionNode>()) {
            const FieldAccessExpressionNodePtr accessExpressionNode = (*itr)->as<FieldAccessExpressionNode>();
            NES_TRACE("FilterPushDownRule: Check if the input field name is same as the FieldAccessExpression field name");
            if (accessExpressionNode->getFieldName() == fieldName) {
                return true;
            }
        }
    }
    return false;
}

std::string FilterPushDownRule::getFieldNameUsedByMapOperator(const NodePtr& node) {
    NES_TRACE("FilterPushDownRule: Find the field name used in map operator");
    MapLogicalOperatorNodePtr mapLogicalOperatorNodePtr = node->as<MapLogicalOperatorNode>();
    const FieldAssignmentExpressionNodePtr mapExpression = mapLogicalOperatorNodePtr->getMapExpression();
    const FieldAccessExpressionNodePtr field = mapExpression->getField();
    const std::string mapFieldName = field->getFieldName();
    return mapFieldName;
}

}// namespace NES::Optimizer
