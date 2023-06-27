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

#include <API/Schema.hpp>
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <Nodes/Expressions/FieldAssignmentExpressionNode.hpp>
#include <Nodes/Util/Iterators/DepthFirstNodeIterator.hpp>
#include <Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/JoinLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/MapLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/ProjectionLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/UnionLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/WatermarkAssignerLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Windowing/WindowLogicalOperatorNode.hpp>
#include <Optimizer/QueryRewrite/FilterPushDownRule.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/Logger/Logger.hpp>
#include <Windowing/DistributionCharacteristic.hpp>
#include <Windowing/LogicalJoinDefinition.hpp>
#include <Windowing/LogicalWindowDefinition.hpp>
#include <Windowing/WindowAggregations/WindowAggregationDescriptor.hpp>
#include <Windowing/WindowTypes/ContentBasedWindowType.hpp>
#include <Windowing/WindowTypes/ThresholdWindow.hpp>
#include <Windowing/WindowTypes/TimeBasedWindowType.hpp>
#include <queue>

namespace NES::Optimizer {

FilterPushDownRulePtr FilterPushDownRule::create() { return std::make_shared<FilterPushDownRule>(FilterPushDownRule()); }

FilterPushDownRule::FilterPushDownRule() = default;

QueryPlanPtr FilterPushDownRule::apply(QueryPlanPtr queryPlan) {

    NES_INFO("Applying FilterPushDownRule to query {}", queryPlan->toString());
    const std::vector<OperatorNodePtr> rootOperators = queryPlan->getRootOperators();
    std::set<FilterLogicalOperatorNodePtr> filterOperatorsSet;
    for (const OperatorNodePtr& rootOperator : rootOperators) {
        std::vector<FilterLogicalOperatorNodePtr> filters = rootOperator->getNodesByType<FilterLogicalOperatorNode>();
        filterOperatorsSet.insert(filters.begin(), filters.end());
    }
    std::vector<FilterLogicalOperatorNodePtr> filterOperators(filterOperatorsSet.begin(), filterOperatorsSet.end());
    NES_DEBUG("FilterPushDownRule: Sort all filter nodes in increasing order of the operator id");
    std::sort(filterOperators.begin(),
              filterOperators.end(),
              [](const FilterLogicalOperatorNodePtr& lhs, const FilterLogicalOperatorNodePtr& rhs) {
                  return lhs->getId() < rhs->getId();
              });
    NES_DEBUG("FilterPushDownRule: Iterate over all the filter operators to push them down in the query plan");
    for (FilterLogicalOperatorNodePtr filterOperator : filterOperators) {
        //method calls itself recursively until it can not push the filter further down(upstream).
        pushDownFilter(filterOperator, filterOperator->getChildren()[0], filterOperator);
    }
    NES_INFO("FilterPushDownRule: Return the updated query plan {}", queryPlan->toString());
    return queryPlan;
}

void FilterPushDownRule::pushDownFilter(FilterLogicalOperatorNodePtr filterOperator, NodePtr curOperator, NodePtr parOperator) {

    // keeps track if we were able to push the filter below another operator.
    bool pushed = false;

    if (curOperator->instanceOf<ProjectionLogicalOperatorNode>()) {
        // TODO: implement filter pushdown for projection: https://github.com/nebulastream/nebulastream/issues/3799
    } else if (curOperator->instanceOf<MapLogicalOperatorNode>()) {
        std::string mapFieldName = getFieldNameUsedByMapOperator(curOperator);
        bool predicateFieldManipulated = isFieldUsedInFilterPredicate(filterOperator, mapFieldName);

        //This map operator does not manipulate the same field that the filter uses, so we are able to push the filter below this operator.
        if (!predicateFieldManipulated) {
            pushed = true;
            pushDownFilter(filterOperator, curOperator->getChildren()[0], curOperator);
        }
    } else if (curOperator->instanceOf<JoinLogicalOperatorNode>()) {
        //field names that are used by the filter
        std::vector<std::string> predicateFields = filterOperator->getFieldNamesUsedByFilterPredicate();

        //if there is only one attribute accessed it might be part of the join
        JoinLogicalOperatorNodePtr curOperatorAsJoin = curOperator->as<JoinLogicalOperatorNode>();
        Join::LogicalJoinDefinitionPtr a = curOperatorAsJoin->getJoinDefinition();

        //if any inputSchema contains all the fields that are used by the filter we can push the filter to the corresponding site
        SchemaPtr leftSchema = curOperator->as<JoinLogicalOperatorNode>()->getLeftInputSchema();
        SchemaPtr rightSchema = curOperator->as<JoinLogicalOperatorNode>()->getRightInputSchema();
        bool leftBranchPossible = true;
        bool rightBranchPossible = true;

        //if any field that is used by the filter is not part of the branches schema, we can't push the filter to that branch.
        for (const std::string& fieldUsedByFilter : predicateFields) {
            if (!leftSchema->contains(fieldUsedByFilter)) {
                leftBranchPossible = false;
            }
            if (!rightSchema->contains(fieldUsedByFilter)) {
                rightBranchPossible = false;
            }
        }

        if (leftBranchPossible) {
            pushed = true;
            pushDownFilter(filterOperator, curOperator->as<JoinLogicalOperatorNode>()->getLeftOperators()[0], curOperator);
        }
        if (rightBranchPossible) {
            pushed = true;
            pushDownFilter(filterOperator, curOperator->as<JoinLogicalOperatorNode>()->getRightOperators()[0], curOperator);
        }

    } else if (curOperator->instanceOf<UnionLogicalOperatorNode>()) {
        std::vector<NodePtr> grandChildren = curOperator->getChildren();

        if (grandChildren.size() != 2) {
            NES_ERROR("FilterPushDownRule: Union should have exactly two children");
            throw std::logic_error("FilterPushDownRule: Union should have exactly two children");
        }

        NodePtr filterOperatorCopy = filterOperator->copy();
        filterOperatorCopy->as<FilterLogicalOperatorNode>()->setId(Util::getNextOperatorId());

        pushed = true;
        pushDownFilter(filterOperator, grandChildren[0], curOperator);
        pushDownFilter(filterOperatorCopy->as<FilterLogicalOperatorNode>(), grandChildren[1], curOperator);
    } else if (curOperator->instanceOf<WindowLogicalOperatorNode>()) {
        auto groupByKeyNames = curOperator->as<WindowLogicalOperatorNode>()->getGroupByKeyNames();
        std::vector<FieldAccessExpressionNodePtr> groupByKeys =
            curOperator->as<WindowLogicalOperatorNode>()->getWindowDefinition()->getKeys();
        std::vector<std::string> fieldNamesUsedByFilter = filterOperator->getFieldNamesUsedByFilterPredicate();
        auto areAllFilterAttributesInGroupByKeys = true;
        for (const auto& filterAttribute : fieldNamesUsedByFilter) {
            if (std::find(groupByKeyNames.begin(), groupByKeyNames.end(), filterAttribute) == groupByKeyNames.end()) {
                areAllFilterAttributesInGroupByKeys = false;
            }
        }
        if (areAllFilterAttributesInGroupByKeys) {
            pushed = true;
            pushDownFilter(filterOperator, curOperator->getChildren()[0], curOperator);
        }
    } else if (curOperator->instanceOf<WatermarkAssignerLogicalOperatorNode>()) {
        pushed = true;
        pushDownFilter(filterOperator, curOperator->getChildren()[0], curOperator);
    }
    // if we were not able to push the filter below the current operator, we now need to insert it in the new position, if the position is changed
    if (!pushed) {
        insertFilterIntoNewPosition(filterOperator, curOperator, parOperator);
    }
}

void FilterPushDownRule::insertFilterIntoNewPosition(FilterLogicalOperatorNodePtr filterOperator,
                                                     NodePtr childOperator,
                                                     NodePtr parOperator) {

    // If the parent operator of the current operator is not the original filter operator, the filter has been pushed below some operators.
    // so we have to remove it from its original position and insert at the new position (above the current operator, which it can't be pushed below)
    if (filterOperator->getId() != parOperator->as<LogicalOperatorNode>()->getId()) {
        // removes filter operator from its original position and connects the parents and children of the filter operator at that position
        if (!filterOperator->removeAndJoinParentAndChildren()) {
            //if we did not manage to remove the operator we can't insert it at the new position
            NES_WARNING("FilterPushDownRule wanted to change the position of a filter, but was not able to do so.");
            return;
        }

        // inserts the operator between the operator that it wasn't able to push below and the parent of this operator.
        bool success1 = childOperator->removeParent(parOperator);// also removes childOperator as a child from parOperator
        bool success2 = childOperator->addParent(filterOperator);// also adds childOperator as a child to filterOperator
        bool success3 = filterOperator->addParent(parOperator);  // also adds filterOperator as a child to parOperator
        if (!success1 || !success2 || !success3) {
            //if we did not manage to remove the operator but now the insertion is not successful, the queryPlan is invalid now
            NES_ERROR(
                "FilterPushDownRule removed a Filter from a query plan but was not able to insert it into the query plan again.");
            throw std::logic_error("FilterPushDownRule: query plan not valid anymore");
        }
    }
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
    return field->getFieldName();
}

}// namespace NES::Optimizer
