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
#include <Operators/Expressions/BinaryExpressionNode.hpp>
#include <Operators/Expressions/FieldAccessExpressionNode.hpp>
#include <Operators/Expressions/FieldAssignmentExpressionNode.hpp>
#include <Operators/Expressions/FieldRenameExpressionNode.hpp>
#include <Nodes/Iterators/DepthFirstNodeIterator.hpp>
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
    auto originalQueryPlan = queryPlan->copy();
    try {
        NES_DEBUG("FilterPushDownRule: Iterate over all the filter operators to push them down in the query plan");
        for (FilterLogicalOperatorNodePtr filterOperator : filterOperators) {
            //method calls itself recursively until it can not push the filter further down(upstream).
            pushDownFilter(filterOperator, filterOperator->getChildren()[0], filterOperator);
        }
        NES_INFO("FilterPushDownRule: Return the updated query plan {}", queryPlan->toString());
        return queryPlan;
    } catch (std::exception& exc) {
        NES_ERROR("FilterPushDownRule: Error while applying FilterPushDownRule: {}", exc.what());
        NES_ERROR("FilterPushDownRule: Returning unchanged original query plan {}", originalQueryPlan->toString());
        return originalQueryPlan;
    }
}

void FilterPushDownRule::pushDownFilter(FilterLogicalOperatorNodePtr filterOperator, NodePtr curOperator, NodePtr parOperator) {

    if (curOperator->instanceOf<ProjectionLogicalOperatorNode>()) {
        pushBelowProjection(filterOperator, curOperator);
    } else if (curOperator->instanceOf<MapLogicalOperatorNode>()) {
        auto mapOperator = curOperator->as<MapLogicalOperatorNode>();
        pushFilterBelowMap(filterOperator, mapOperator);
    } else if (curOperator->instanceOf<JoinLogicalOperatorNode>()) {
        auto joinOperator = curOperator->as<JoinLogicalOperatorNode>();
        pushFilterBelowJoin(filterOperator, joinOperator, parOperator);
    } else if (curOperator->instanceOf<UnionLogicalOperatorNode>()) {
        pushFilterBelowUnion(filterOperator, curOperator);
    } else if (curOperator->instanceOf<WindowLogicalOperatorNode>()) {
        pushFilterBelowWindowAggregation(filterOperator, curOperator, parOperator);
    } else if (curOperator->instanceOf<WatermarkAssignerLogicalOperatorNode>()) {
        pushDownFilter(filterOperator, curOperator->getChildren()[0], curOperator);
    }
    // if we have a source operator or some unsupported operator we are not able to push the filter below this operator.
    else {
        insertFilterIntoNewPosition(filterOperator, curOperator, parOperator);
    }
}

void FilterPushDownRule::pushFilterBelowJoin(FilterLogicalOperatorNodePtr filterOperator,
                                             JoinLogicalOperatorNodePtr joinOperator,
                                             NES::NodePtr parentOperator) {
    // we might be able to push the filter to both branches of the join, and we check this first.
    bool pushed = pushFilterBelowJoinSpecialCase(filterOperator, joinOperator);

    //field names that are used by the filter
    std::vector<std::string> predicateFields = filterOperator->getFieldNamesUsedByFilterPredicate();
    //better readability
    JoinLogicalOperatorNodePtr curOperatorAsJoin = joinOperator->as<JoinLogicalOperatorNode>();
    // we might have pushed it already within the special condition before
    if (!pushed) {
        //if any inputSchema contains all the fields that are used by the filter we can push the filter to the corresponding site
        SchemaPtr leftSchema = curOperatorAsJoin->getLeftInputSchema();
        SchemaPtr rightSchema = curOperatorAsJoin->getRightInputSchema();
        bool leftBranchPossible = true;
        bool rightBranchPossible = true;

        //if any field that is used by the filter is not part of the branches schema, we can't push the filter to that branch.
        NES_DEBUG("FilterPushDownRule.pushFilterBelowJoin: Checking if filter can be pushed below left or right branch of join");
        for (const std::string& fieldUsedByFilter : predicateFields) {
            if (!leftSchema->contains(fieldUsedByFilter)) {
                leftBranchPossible = false;
            }
            if (!rightSchema->contains(fieldUsedByFilter)) {
                rightBranchPossible = false;
            }
        }

        if (leftBranchPossible) {
            NES_DEBUG("FilterPushDownRule.pushFilterBelowJoin: Pushing filter below left branch of join");
            pushed = true;
            pushDownFilter(filterOperator, curOperatorAsJoin->getLeftOperators()[0], joinOperator);
        } else if (rightBranchPossible) {
            NES_DEBUG("FilterPushDownRule.pushFilterBelowJoin: Pushing filter below right branch of join");
            pushed = true;
            pushDownFilter(filterOperator, curOperatorAsJoin->getRightOperators()[0], joinOperator);
        }
    }

    if (!pushed) {
        NES_DEBUG("FilterPushDownRule.pushFilterBelowJoin: Filter was not pushed, we insert filter at this position");
        insertFilterIntoNewPosition(filterOperator, joinOperator, parentOperator);
    }
}

bool FilterPushDownRule::pushFilterBelowJoinSpecialCase(FilterLogicalOperatorNodePtr filterOperator,
                                                        JoinLogicalOperatorNodePtr joinOperator) {

    std::vector<std::string> predicateFields = filterOperator->getFieldNamesUsedByFilterPredicate();
    Join::LogicalJoinDefinitionPtr joinDefinition = joinOperator->getJoinDefinition();
    NES_DEBUG("FilterPushDownRule.pushFilterBelowJoinSpecialCase: Extracted field names that are used by the filter");

    if (joinDefinition->getLeftJoinKey()->getFieldName() == predicateFields[0]) {
        NES_DEBUG("FilterPushDownRule.pushFilterBelowJoinSpecialCase: Filter field name found in the left side of the join");
        auto copyOfFilter = filterOperator->copy()->as<FilterLogicalOperatorNode>();
        copyOfFilter->setId(Util::getNextOperatorId());
        NES_DEBUG("FilterPushDownRule.pushFilterBelowJoinSpecialCase: Created a copy of the filter");

        ExpressionNodePtr newPredicate = filterOperator->getPredicate()->copy();

        renameFieldAccessExpressionNodes(newPredicate,
                                         joinDefinition->getLeftJoinKey()->getFieldName(),
                                         joinDefinition->getRightJoinKey()->getFieldName());

        copyOfFilter->setPredicate(newPredicate);
        NES_DEBUG("FilterPushDownRule.pushFilterBelowJoinSpecialCase: Set the right side field name to the predicate field");

        pushDownFilter(filterOperator, joinOperator->getLeftOperators()[0], joinOperator);
        NES_DEBUG("FilterPushDownRule.pushFilterBelowJoinSpecialCase: Pushed original filter to one branch");
        pushDownFilter(copyOfFilter, joinOperator->getRightOperators()[0], joinOperator);
        NES_DEBUG("FilterPushDownRule.pushFilterBelowJoinSpecialCase: Pushed copy of the filter to other branch");
        return true;
    } else if (joinDefinition->getRightJoinKey()->getFieldName() == predicateFields[0]) {
        NES_DEBUG("FilterPushDownRule.pushFilterBelowJoinSpecialCase: Filter field name found in the right side of the join");
        auto copyOfFilter = filterOperator->copy()->as<FilterLogicalOperatorNode>();
        copyOfFilter->setId(Util::getNextOperatorId());
        NES_DEBUG("FilterPushDownRule.pushFilterBelowJoinSpecialCase: Created a copy of the filter");

        ExpressionNodePtr newPredicate = filterOperator->getPredicate()->copy();
        renameFieldAccessExpressionNodes(newPredicate,
                                         joinDefinition->getRightJoinKey()->getFieldName(),
                                         joinDefinition->getLeftJoinKey()->getFieldName());
        copyOfFilter->setPredicate(newPredicate);
        NES_DEBUG("FilterPushDownRule.pushFilterBelowJoinSpecialCase: Set the left side field name to the predicate field");

        pushDownFilter(filterOperator, joinOperator->getRightOperators()[0], joinOperator);
        NES_DEBUG("FilterPushDownRule.pushFilterBelowJoinSpecialCase: Pushed original filter to one branch");
        pushDownFilter(copyOfFilter, joinOperator->getLeftOperators()[0], joinOperator);
        NES_DEBUG("FilterPushDownRule.pushFilterBelowJoinSpecialCase: Pushed copy of the filter to other branch");
        return true;
    }
    return false;
}

void FilterPushDownRule::pushFilterBelowMap(FilterLogicalOperatorNodePtr filterOperator, MapLogicalOperatorNodePtr mapOperator) {
    std::string assignmentFieldName = getAssignmentFieldFromMapOperator(mapOperator);
    substituteFilterAttributeWithMapTransformation(filterOperator, mapOperator, assignmentFieldName);
    pushDownFilter(filterOperator, mapOperator->getChildren()[0], mapOperator);
}

void FilterPushDownRule::pushFilterBelowUnion(FilterLogicalOperatorNodePtr filterOperator, NodePtr unionOperator) {
    std::vector<NodePtr> grandChildren = unionOperator->getChildren();

    auto copyOfFilterOperator = filterOperator->copy()->as<FilterLogicalOperatorNode>();
    copyOfFilterOperator->setId(Util::getNextOperatorId());
    copyOfFilterOperator->setPredicate(filterOperator->getPredicate()->copy());
    NES_DEBUG("FilterPushDownRule.pushFilterBelowUnion: Created a copy of the filter operator");

    NES_DEBUG("FilterPushDownRule.pushFilterBelowUnion: Pushing filter into both branches below union operator...");
    pushDownFilter(filterOperator, grandChildren[0], unionOperator);
    NES_DEBUG("FilterPushDownRule.pushFilterBelowUnion: Pushed original filter to one branch");
    pushDownFilter(copyOfFilterOperator, grandChildren[1], unionOperator);
    NES_DEBUG("FilterPushDownRule.pushFilterBelowUnion: Pushed copy of the filter to other branch");
}

void FilterPushDownRule::pushFilterBelowWindowAggregation(FilterLogicalOperatorNodePtr filterOperator,
                                                          NodePtr windowOperator,
                                                          NodePtr parOperator) {

    auto groupByKeyNames = windowOperator->as<WindowLogicalOperatorNode>()->getGroupByKeyNames();
    std::vector<FieldAccessExpressionNodePtr> groupByKeys =
        windowOperator->as<WindowLogicalOperatorNode>()->getWindowDefinition()->getKeys();
    std::vector<std::string> fieldNamesUsedByFilter = filterOperator->getFieldNamesUsedByFilterPredicate();
    NES_DEBUG("FilterPushDownRule.pushFilterBelowWindowAggregation: Retrieved the group by keys of the window operator");

    bool areAllFilterAttributesInGroupByKeys = true;
    for (const auto& filterAttribute : fieldNamesUsedByFilter) {
        if (std::find(groupByKeyNames.begin(), groupByKeyNames.end(), filterAttribute) == groupByKeyNames.end()) {
            areAllFilterAttributesInGroupByKeys = false;
        }
    }
    NES_DEBUG("FilterPushDownRule.pushFilterBelowWindowAggregation: Checked if all attributes used by the filter are also used "
              "in the group by keys the group by keys of the window operator");

    if (areAllFilterAttributesInGroupByKeys) {
        NES_DEBUG("FilterPushDownRule.pushFilterBelowWindowAggregation: All attributes used by the filter are also used in the"
                  " group by keys, we can push the filter below the window operator")
        NES_DEBUG("FilterPushDownRule.pushFilterBelowWindowAggregation: Pushing filter below window operator...");
        pushDownFilter(filterOperator, windowOperator->getChildren()[0], windowOperator);
    } else {
        NES_DEBUG("FilterPushDownRule.pushFilterBelowWindowAggregation: Attributes used by filter are not all used in "
                  "group by keys, inserting the filter into a new position...");
        insertFilterIntoNewPosition(filterOperator, windowOperator, parOperator);
    }
}

void FilterPushDownRule::pushBelowProjection(FilterLogicalOperatorNodePtr filterOperator, NodePtr projectionOperator) {
    renameFilterAttributesByExpressionNodes(filterOperator,
                                            projectionOperator->as<ProjectionLogicalOperatorNode>()->getExpressions());
    pushDownFilter(filterOperator, projectionOperator->getChildren()[0], projectionOperator);
}

void FilterPushDownRule::insertFilterIntoNewPosition(FilterLogicalOperatorNodePtr filterOperator,
                                                     NodePtr childOperator,
                                                     NodePtr parOperator) {

    // If the parent operator of the current operator is not the original filter operator, the filter has been pushed below some operators.
    // so we have to remove it from its original position and insert at the new position (above the current operator, which it can't be pushed below)
    if (filterOperator->getId() != parOperator->as<LogicalOperatorNode>()->getId()) {

        // if we remove the first child (which is the left branch) of a binary operator and insert our filter below this binary operator,
        // it will be inserted as the second children (which is the right branch). To conserve order we will swap the branches after the insertion
        bool swapBranches = false;
        if (parOperator->instanceOf<BinaryOperatorNode>() && parOperator->getChildren()[0]->equal(childOperator)) {
            swapBranches = true;
        }

        // removes filter operator from its original position and connects the parents and children of the filter operator at that position
        if (!filterOperator->removeAndJoinParentAndChildren()) {
            //if we did not manage to remove the operator we can't insert it at the new position
            NES_WARNING("FilterPushDownRule wanted to change the position of a filter, but was not able to do so.")
            return;
        }

        // inserts the operator between the operator that it wasn't able to push below and the parent of this operator.
        bool success1 = childOperator->removeParent(parOperator);// also removes childOperator as a child from parOperator
        bool success2 = childOperator->addParent(filterOperator);// also adds childOperator as a child to filterOperator
        bool success3 = filterOperator->addParent(parOperator);  // also adds filterOperator as a child to parOperator
        if (!success1 || !success2 || !success3) {
            //if we did manage to remove the filter from the queryPlan but now the insertion is not successful, that means that the queryPlan is invalid now.
            NES_ERROR(
                "FilterPushDownRule removed a Filter from a query plan but was not able to insert it into the query plan again.")
            throw std::logic_error("FilterPushDownRule: query plan not valid anymore");
        }

        //the input schema of the filter is going to be the same as the output schema of the node below. Its output schema is the same as its input schema.
        filterOperator->setInputSchema(filterOperator->getChildren()[0]->as<OperatorNode>()->getOutputSchema()->copy());
        filterOperator->as<OperatorNode>()->setOutputSchema(
            filterOperator->getChildren()[0]->as<OperatorNode>()->getOutputSchema()->copy());

        //conserve order
        if (swapBranches) {
            parOperator->swapLeftAndRightBranch();
        }
    }
}

std::vector<FieldAccessExpressionNodePtr>
FilterPushDownRule::getFilterAccessExpressions(const ExpressionNodePtr& filterPredicate) {
    std::vector<FieldAccessExpressionNodePtr> filterAccessExpressions;
    NES_TRACE("FilterPushDownRule: Create an iterator for traversing the filter predicates");
    DepthFirstNodeIterator depthFirstNodeIterator(filterPredicate);
    for (auto itr = depthFirstNodeIterator.begin(); itr != NES::DepthFirstNodeIterator::end(); ++itr) {
        NES_TRACE("FilterPushDownRule: Iterate and find the predicate with FieldAccessExpression Node");
        if ((*itr)->instanceOf<FieldAccessExpressionNode>()) {
            const FieldAccessExpressionNodePtr accessExpressionNode = (*itr)->as<FieldAccessExpressionNode>();
            NES_TRACE("FilterPushDownRule: Add the field name to the list of filter attribute names");
            filterAccessExpressions.push_back(accessExpressionNode);
        }
    }
    return filterAccessExpressions;
}

void FilterPushDownRule::renameFieldAccessExpressionNodes(ExpressionNodePtr expressionNode,
                                                          const std::string toReplace,
                                                          const std::string replacement) {
    DepthFirstNodeIterator depthFirstNodeIterator(expressionNode);
    for (auto itr = depthFirstNodeIterator.begin(); itr != NES::DepthFirstNodeIterator::end(); ++itr) {
        if ((*itr)->instanceOf<FieldAccessExpressionNode>()) {
            const FieldAccessExpressionNodePtr accessExpressionNode = (*itr)->as<FieldAccessExpressionNode>();
            if (accessExpressionNode->getFieldName() == toReplace) {
                accessExpressionNode->updateFieldName(replacement);
            }
        }
    }
}

void FilterPushDownRule::renameFilterAttributesByExpressionNodes(const FilterLogicalOperatorNodePtr& filterOperator,
                                                                 const std::vector<ExpressionNodePtr>& expressionNodes) {
    ExpressionNodePtr predicateCopy = filterOperator->getPredicate()->copy();
    NES_TRACE("FilterPushDownRule: Iterate over all expressions in the projection operator");

    for (auto& expressionNode : expressionNodes) {
        NES_TRACE("FilterPushDownRule: Check if the expression node is of type FieldRenameExpressionNode")
        if (expressionNode->instanceOf<FieldRenameExpressionNode>()) {
            FieldRenameExpressionNodePtr fieldRenameExpressionNode = expressionNode->as<FieldRenameExpressionNode>();
            std::string newFieldName = fieldRenameExpressionNode->getNewFieldName();
            std::string originalFieldName = fieldRenameExpressionNode->getOriginalField()->getFieldName();
            renameFieldAccessExpressionNodes(predicateCopy, newFieldName, originalFieldName);
        }
    }

    filterOperator->setPredicate(predicateCopy);
}

std::string FilterPushDownRule::getAssignmentFieldFromMapOperator(const NodePtr& node) {
    NES_TRACE("FilterPushDownRule: Find the field name used in map operator");
    MapLogicalOperatorNodePtr mapLogicalOperatorNodePtr = node->as<MapLogicalOperatorNode>();
    const FieldAssignmentExpressionNodePtr mapExpression = mapLogicalOperatorNodePtr->getMapExpression();
    const FieldAccessExpressionNodePtr field = mapExpression->getField();
    return field->getFieldName();
}

void FilterPushDownRule::substituteFilterAttributeWithMapTransformation(const FilterLogicalOperatorNodePtr& filterOperator,
                                                                        const MapLogicalOperatorNodePtr& mapOperator,
                                                                        const std::string& fieldName) {
    NES_TRACE("Substitute filter predicate's field access expression with map transformation.");
    NES_TRACE("Current map transformation: {}", mapOperator->getMapExpression()->toString());
    const FieldAssignmentExpressionNodePtr mapExpression = mapOperator->getMapExpression();
    const ExpressionNodePtr mapTransformation = mapExpression->getAssignment();
    std::vector<FieldAccessExpressionNodePtr> filterAccessExpressions =
        getFilterAccessExpressions(filterOperator->getPredicate());
    // iterate over all filter access expressions
    // replace the filter predicate's field access expression with map transformation
    // if the field name of the field access expression is the same as the field name used in map transformation
    for (auto& filterAccessExpression : filterAccessExpressions) {
        if (filterAccessExpression->getFieldName() == fieldName) {
            filterAccessExpression->replace(mapTransformation->copy());
        }
    }
    NES_TRACE("New filter predicate: {}", filterOperator->getPredicate()->toString());
}
}// namespace NES::Optimizer
