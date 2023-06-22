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
#include <queue>

namespace NES::Optimizer {

FilterPushDownRulePtr FilterPushDownRule::create() { return std::make_shared<FilterPushDownRule>(FilterPushDownRule()); }

FilterPushDownRule::FilterPushDownRule() = default;

QueryPlanPtr FilterPushDownRule::apply(QueryPlanPtr queryPlan) {

    NES_INFO2("Applying FilterPushDownRule to query {}", queryPlan->toString());
    const auto rootOperators = queryPlan->getRootOperators();
    std::vector<FilterLogicalOperatorNodePtr> filterOperators;
    for (const auto& rootOperator : rootOperators) {
        //FIXME: this will result in adding same filter operator twice in the vector
        // remove the duplicate filters from the vector
        auto filters = rootOperator->getNodesByType<FilterLogicalOperatorNode>();
        filterOperators.insert(filterOperators.end(), filters.begin(), filters.end());
    }
    NES_DEBUG2("FilterPushDownRule: Sort all filter nodes in increasing order of the operator id");
    std::sort(filterOperators.begin(),
              filterOperators.end(),
              [](const FilterLogicalOperatorNodePtr& lhs, const FilterLogicalOperatorNodePtr& rhs) {
                  return lhs->getId() < rhs->getId();
              });
    NES_DEBUG2("FilterPushDownRule: Iterate over all the filter operators to push them down in the query plan");
    for (const auto& filterOperator : filterOperators) {
        //method calls itself recursively until it can not push the filter further down(upstream).
        pushDownFilter(filterOperator, filterOperator->getChildren()[0], filterOperator);
    }
    NES_INFO2("FilterPushDownRule: Return the updated query plan {}", queryPlan->toString());
    return queryPlan;
}

void FilterPushDownRule::pushDownFilter(const FilterLogicalOperatorNodePtr& filterOperator, const NodePtr& curOperator, const NodePtr& parOperator) {

    // keeps track if we were able to push the filter below another operator.
    bool pushed = false;

    if (curOperator->instanceOf<ProjectionLogicalOperatorNode>()) {
        // TODO: implement filter pushdown for projection: https://github.com/nebulastream/nebulastream/issues/3799
    }
    else if (curOperator->instanceOf<MapLogicalOperatorNode>()) {
        std::string mapFieldName = getFieldNameUsedByMapOperator(curOperator);
        bool predicateFieldManipulated = isFieldUsedInFilterPredicate(filterOperator, mapFieldName);

        //This map operator does not manipulate the same field that the filter uses, so we are able to push the filter below this operator.
        if (!predicateFieldManipulated) {
            pushed = true;
            pushDownFilter(filterOperator, curOperator->getChildren()[0], curOperator);
        }
    }
    else if (curOperator->instanceOf<JoinLogicalOperatorNode>()) {
        //field names that are used by the filter
        std::vector<std::string> predicateFields = filterOperator->getFieldNamesUsedByFilterPredicate();

        //if any inputSchema contains all the fields that are used by the filter we can push the filter to the corresponding site
        SchemaPtr leftSchema = curOperator->as<JoinLogicalOperatorNode>()->getLeftInputSchema();
        SchemaPtr rightSchema = curOperator->as<JoinLogicalOperatorNode>()->getRightInputSchema();
        bool leftBranchPossible = true;
        bool rightBranchPossible = true;

        //if any field that is used by the filter is not part of the branches schema, we can't push the filter to that branch.
        for (const std::string& fieldUsedByFilter: predicateFields){
            if (!leftSchema->contains(fieldUsedByFilter)) {leftBranchPossible = false;}
            if (!rightSchema->contains(fieldUsedByFilter)) {rightBranchPossible = false;}
        }

        if (leftBranchPossible) {
            pushed = true;
            pushDownFilter(filterOperator,curOperator->as<JoinLogicalOperatorNode>()->getLeftOperators()[0],curOperator);
        }
        if (rightBranchPossible) {
            pushed = true;
            pushDownFilter(filterOperator, curOperator->as<JoinLogicalOperatorNode>()->getRightOperators()[0],curOperator);
        }

    }
    else if (curOperator->instanceOf<UnionLogicalOperatorNode>()) {
        std::vector<NodePtr> grandChildren = curOperator->getChildren();

        if (grandChildren.size() != 2) {
            NES_ERROR2("FilterPushDownRule: Union should have exactly two children");
            throw std::logic_error("FilterPushDownRule: Union should have exactly two children");
        }

        NodePtr filterOperatorCopy = filterOperator->copy();
        filterOperatorCopy->as<FilterLogicalOperatorNode>()->setId(Util::getNextOperatorId());

        //otherwise the order of the branches below the union is not consistent. This would make it harder to write tests, but it should actually be fine if the order changes.
        insertFilterIntoNewPosition(filterOperator, grandChildren[0], curOperator);
        insertFilterIntoNewPosition(filterOperatorCopy->as<FilterLogicalOperatorNode>(), grandChildren[1], curOperator);

        pushed = true;
        pushDownFilter(filterOperator, grandChildren[0], curOperator);
        pushDownFilter(filterOperatorCopy->as<FilterLogicalOperatorNode>(), grandChildren[1], curOperator);
    }
    else if (curOperator->instanceOf<WindowLogicalOperatorNode>()) {
        // TODO: implement filter pushdown for window aggregations: https://github.com/nebulastream/nebulastream/issues/3804
    }
    else if (curOperator->instanceOf<WatermarkAssignerLogicalOperatorNode>()) {
        pushed = true;
        pushDownFilter(filterOperator, curOperator->getChildren()[0], curOperator);
    }
    // if we were not able to push the filter below the current operator, we now need to insert it in the new position, if the position is changed
    if (!pushed) {
        insertFilterIntoNewPosition(filterOperator, curOperator, parOperator);
    }
}

void FilterPushDownRule::insertFilterIntoNewPosition(const FilterLogicalOperatorNodePtr& filterOperator, const NodePtr& childOperator, const NodePtr& parOperator){

    // If the parent operator of the current operator is not the original filter operator, the filter has been pushed below some operators.
    // so we have to remove it from its original position and insert at the new position (above the current operator, which it can't be pushed below)
    if (filterOperator->getId() != parOperator->as<LogicalOperatorNode>()->getId()) {
        // removes filter operator from its original position and connects the parents and children of the filter operator at that position
        if (!filterOperator->removeAndJoinParentAndChildren()){
            //if we did not manage to remove the operator we can't insert it at the new position
            return;
        }

        // inserts the operator above the operator that it wasn't able to push below.
        // This can only be done if that operator does not have multiple parents as the filter operator will get insert below every parent of this operator.
        if (!childOperator->as<LogicalOperatorNode>()->hasMultipleParents()){
            if (!childOperator->insertBetweenThisAndParentNodes(filterOperator)){
                //if we did not manage to remove the operator but now the insertion is not successful, the queryPlan is invalid now
                throw std::logic_error("FilterPushDownRule: query plan not valid anymore");
            }
        }
        // inserts the operator below the parent operator of the operator that it wasn't able to push below.
        // This can only be done if that operator does not have multiple children as the filter operator will get insert above every child of this operator.
        else if (!parOperator->as<LogicalOperatorNode>()->hasMultipleChildren()){
            if (!parOperator->insertBetweenThisAndChildNodes(filterOperator)){
                //if we did not manage to remove the operator but now the insertion is not successful, the queryPlan is invalid now
                throw std::logic_error("FilterPushDownRule: query plan not valid anymore");
            }
        }
        else {
            // This probably works.
            // BUT there were weird bugs, if we use this method in the case that there is only one parent or one child.
            // The pointers themselves(childOperator, parOperator) pointed to a different object than the original object after adding or removing a child/parent.
            // Maybe this has something to do with the operator getting completely disconnected from the queryPlan, but the actual reason isn't obvious.
            bool success1 = childOperator->removeParent(parOperator);// also removes childOperator as a child from parOperator
            bool success2 = childOperator->addParent(filterOperator); // also adds childOperator as a child to filterOperator
            bool success3 = filterOperator->addParent(parOperator); // also adds filterOperator as a child to parOperator
            if (!success1 || !success2 || !success3){
                //if we did not manage to remove the operator but now the insertion is not successful, the queryPlan is invalid now
                throw std::logic_error("FilterPushDownRule: query plan not valid anymore");
            }
        }
    }
}

bool FilterPushDownRule::isFieldUsedInFilterPredicate(FilterLogicalOperatorNodePtr const& filterOperator,
                                                      const std::string& fieldName) {

    NES_TRACE2("FilterPushDownRule: Create an iterator for traversing the filter predicates");
    const ExpressionNodePtr filterPredicate = filterOperator->getPredicate();
    DepthFirstNodeIterator depthFirstNodeIterator(filterPredicate);
    for (auto itr = depthFirstNodeIterator.begin(); itr != NES::DepthFirstNodeIterator::end(); ++itr) {
        NES_TRACE2("FilterPushDownRule: Iterate and find the predicate with FieldAccessExpression Node");
        if ((*itr)->instanceOf<FieldAccessExpressionNode>()) {
            const FieldAccessExpressionNodePtr accessExpressionNode = (*itr)->as<FieldAccessExpressionNode>();
            NES_TRACE2("FilterPushDownRule: Check if the input field name is same as the FieldAccessExpression field name");
            if (accessExpressionNode->getFieldName() == fieldName) {
                return true;
            }
        }
    }
    return false;
}

std::string FilterPushDownRule::getFieldNameUsedByMapOperator(const NodePtr& node) {
    NES_TRACE2("FilterPushDownRule: Find the field name used in map operator");
    MapLogicalOperatorNodePtr mapLogicalOperatorNodePtr = node->as<MapLogicalOperatorNode>();
    const FieldAssignmentExpressionNodePtr mapExpression = mapLogicalOperatorNodePtr->getMapExpression();
    const FieldAccessExpressionNodePtr field = mapExpression->getField();
    return field->getFieldName();
}

}// namespace NES::Optimizer
