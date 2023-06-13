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

#include "API/Schema.hpp"
#include "API/AttributeField.hpp"
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <Nodes/Expressions/FieldAssignmentExpressionNode.hpp>
#include <Nodes/Util/Iterators/DepthFirstNodeIterator.hpp>
#include <Nodes/Node.hpp>
#include <Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/InferModelLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/JoinLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/MapLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/ProjectionLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/UnionLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Windowing/WindowLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/WatermarkAssignerLogicalOperatorNode.hpp>
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
        pushDownFilter(filterOperator);
    }
    NES_INFO2("FilterPushDownRule: Return the updated query plan {}", queryPlan->toString());
    return queryPlan;
}

void FilterPushDownRule::pushDownFilter(const FilterLogicalOperatorNodePtr& filterOperator) {
    std::vector<NodePtr> childrenOfFilter = filterOperator->getChildren();

    if (childrenOfFilter.empty()) {
        NES_ERROR2("FilterPushDownRule: Filter is the leaf, something is wrong");
        throw std::logic_error("FilterPushDownRule: Filter is the leaf, something is wrong");
    }

    if (childrenOfFilter.size() > 1) {
        NES_ERROR2("FilterPushDownRule: Filter has multiple children, case not handled");
        throw std::logic_error("FilterPushDownRule: Filter has multiple children, case not handled");
    }

    NodePtr child = childrenOfFilter[0];

    if (child->instanceOf<ProjectionLogicalOperatorNode>()) {
        // TODO: implement filter pushdown for projection: https://github.com/nebulastream/nebulastream/issues/3799
    }
    else if (child->instanceOf<MapLogicalOperatorNode>()) {
        std::string mapFieldName = getFieldNameUsedByMapOperator(child);
        bool predicateFieldManipulated = isFieldUsedInFilterPredicate(filterOperator, mapFieldName);
        if (!predicateFieldManipulated) {
            filterOperator->removeAndJoinParentAndChildren();
            child->insertBetweenThisAndChildNodes(filterOperator);
            pushDownFilter(filterOperator);
        }
    }
    else if (child->instanceOf<JoinLogicalOperatorNode>()) {

        std::vector<NodePtr> childrenOfJoin = child->getChildren();

        for (const NodePtr& grandChild : childrenOfJoin) {

            SchemaPtr grandChildOutputSchema = grandChild->as<LogicalOperatorNode>()->getOutputSchema();
            std::vector<std::string> predicateFields = getFieldNamesUsedByFilterPredicate(filterOperator);

            bool thisBranchContainsAllPredicates = true;
            for (const std::string& fieldUsedByFilter: predicateFields){
                if (!grandChildOutputSchema->contains(fieldUsedByFilter)){
                    thisBranchContainsAllPredicates = false;
                }
            }

            if (thisBranchContainsAllPredicates) {

                filterOperator->removeAndJoinParentAndChildren();
                // as watermark is not implemented yet as its own condition,
                // right now I am just pushing directly below the watermark(window) operator below a join
                if (grandChild->instanceOf<WatermarkAssignerLogicalOperatorNode>()) {
                    grandChild->insertBetweenThisAndChildNodes(filterOperator);
                } else {
                    grandChild->insertBetweenThisAndParentNodes(filterOperator);
                }
                pushDownFilter(filterOperator);
            }
        }
    }

    else if (child->instanceOf<UnionLogicalOperatorNode>()) {
        std::vector<NodePtr> grandChildren = child->getChildren();

        if (grandChildren.size() != 2) {
            NES_ERROR2("FilterPushDownRule: Union should have exactly two children");
            throw std::logic_error("FilterPushDownRule: Union should have exactly two children");
        }

        filterOperator->removeAndJoinParentAndChildren();
        NodePtr filterOperatorCopy = filterOperator->copy();

        NodePtr leftChild = grandChildren[0];
        NodePtr rightChild = grandChildren[1];

        leftChild->insertBetweenThisAndParentNodes(filterOperator);
        pushDownFilter(filterOperator);

        rightChild->insertBetweenThisAndParentNodes(filterOperatorCopy);
        pushDownFilter(filterOperatorCopy->as<FilterLogicalOperatorNode>());

    } else if (child->instanceOf<WindowLogicalOperatorNode>()) {
        // TODO: implement filter pushdown for window aggregations: https://github.com/nebulastream/nebulastream/issues/3804
        // Windows can be used either in joins or aggregations.
        // For window joins, the push-down below join policy is applied.
        // For window aggregations, we check for one case in which the filter can be pushed down.
        // Such case happens when there is a group_by clause on a certain attribute
        // and the filter is applied to the same attribute.
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

std::vector<std::string>  FilterPushDownRule::getFieldNamesUsedByFilterPredicate(const NodePtr& node) {
    std::vector<std::string> fieldsInPredicate;

    NES_TRACE2("FilterPushDownRule: Find all field names used in filter operator");
    FilterLogicalOperatorNodePtr filter = node->as<FilterLogicalOperatorNode>();
    const ExpressionNodePtr predicate = filter->getPredicate();

    DepthFirstNodeIterator depthFirstNodeIterator(predicate);
    for (auto itr = depthFirstNodeIterator.begin(); itr != NES::DepthFirstNodeIterator::end(); ++itr) {
        if ((*itr)->instanceOf<FieldAccessExpressionNode>()) {
            const FieldAccessExpressionNodePtr accessExpressionNode = (*itr)->as<FieldAccessExpressionNode>();
            fieldsInPredicate.push_back(accessExpressionNode->getFieldName());
        }
    }

    return fieldsInPredicate;
}

}// namespace NES::Optimizer
