/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#include <API/Windowing.hpp>
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <Nodes/Expressions/FieldAssignmentExpressionNode.hpp>
#include <Nodes/Node.hpp>
#include <Nodes/Util/Iterators/DepthFirstNodeIterator.hpp>
#include <Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/JoinLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/MapLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/MergeLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/WatermarkAssignerLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Windowing/WindowLogicalOperatorNode.hpp>
#include <Optimizer/QueryRewrite/FilterPushDownRule.hpp>
#include <Optimizer/Utils/QuerySignatureUtil.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Windowing/LogicalWindowDefinition.hpp>
#include <Windowing/Watermark/WatermarkStrategyDescriptor.hpp>
#include <Windowing/WindowingForwardRefs.hpp>
#include <queue>

namespace NES {

FilterPushDownRulePtr FilterPushDownRule::create() { return std::make_shared<FilterPushDownRule>(FilterPushDownRule()); }

FilterPushDownRule::FilterPushDownRule() {}

QueryPlanPtr FilterPushDownRule::apply(QueryPlanPtr queryPlanPtr) {

    NES_INFO("FilterPushDownRule: Get all filter nodes in the graph");
    const auto rootOperators = queryPlanPtr->getRootOperators();
    std::vector<FilterLogicalOperatorNodePtr> filterOperators;

    for (auto rootOperator : rootOperators) {
        //FIXME: this will result in adding same filter operator twice in the vector
        // remove the duplicate filters from the vector
        auto filters = rootOperator->getNodesByType<FilterLogicalOperatorNode>();
        filterOperators.insert(filterOperators.end(), filters.begin(), filters.end());
    }

    NES_INFO("FilterPushDownRule: Sort all filter nodes in increasing order of the operator id");

    std::sort(filterOperators.begin(), filterOperators.end(),
              [](FilterLogicalOperatorNodePtr lhs, FilterLogicalOperatorNodePtr rhs) {
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

        static bool isFilterAboveAMergeOperator{false};
        static bool isFilterAboveAJoinOperator{false};
        //isFilterAboveAJoinOperator = isFilterAboveAMergeOperator;
        NES_INFO("FilterPushDownRule: Get first operator for processing");
        NodePtr node = nodesToProcess.front();
        nodesToProcess.pop_front();

        if (node->instanceOf<SourceLogicalOperatorNode>() || node->instanceOf<WindowLogicalOperatorNode>()
            || node->instanceOf<FilterLogicalOperatorNode>()) {

            NES_INFO("FilterPushDownRule: Filter can't be pushed below the " + node->toString() + " operator");

            if (node->as<OperatorNode>()->getId() != filterOperator->getId()) {

                NES_INFO("FilterPushDownRule: Adding Filter operator between current operator and its parents");

                if (isFilterAboveAMergeOperator) {

                    NES_INFO("FilterPushDownRule: Create a duplicate filter operator with new operator ID");
                    OperatorNodePtr duplicatedFilterOperator = filterOperator->copy();
                    duplicatedFilterOperator->setId(UtilityFunctions::getNextOperatorId());

                    if (!(duplicatedFilterOperator->removeAndJoinParentAndChildren()
                          && node->insertBetweenThisAndParentNodes(duplicatedFilterOperator))) {

                        NES_ERROR("FilterPushDownRule: Failure in applying filter push down rule");
                        throw std::logic_error("FilterPushDownRule: Failure in applying filter push down rule");
                    }

                    isFilterAboveAMergeOperator = false;
                    continue;
                }
                if (!(filterOperator->removeAndJoinParentAndChildren()
                      && node->insertBetweenThisAndParentNodes(filterOperator->copy()))) {

                    NES_ERROR("FilterPushDownRule: Failure in applying filter push down rule");
                    throw std::logic_error("FilterPushDownRule: Failure in applying filter push down rule");
                }
                isFilterAboveAJoinOperator = false; //ToDo::Move this
                continue;
            }
        } else if (node->instanceOf<MapLogicalOperatorNode>()) {

            std::string mapFieldName = getFieldNameUsedByMapOperator(node);
            bool predicateFieldManipulated = isFieldUsedInFilterPredicate(filterOperator, mapFieldName);

            if (predicateFieldManipulated) {
                OperatorNodePtr copyOptr = filterOperator->duplicate();
                std::vector<NodePtr> parentsOfNode = node->as<OperatorNode>()->getParents();
                NodePtr nodeExists;
                for (auto&& currentNode : parentsOfNode) {
                    if (copyOptr->equal(currentNode)) {
                        nodeExists = currentNode;
                    }
                }
                if (nodeExists!= nullptr) {
                    NES_WARNING("Node: the node is already part of its parents so ignore insertBetweenThisAndParentNodes operation.");
                    break;
                    //continue;
                } else if (!(copyOptr->removeAndJoinParentAndChildren() && node->insertBetweenThisAndParentNodes(copyOptr))) {

                    NES_ERROR("FilterPushDownRule: Failure in applying filter push down rule");
                    throw std::logic_error("FilterPushDownRule: Failure in applying filter push down rule");
                }
                continue;
            } else {
                std::vector<NodePtr> children = node->getChildren();
                if (isFilterAboveAMergeOperator) {//To ensure duplicated filter operator with a new operator ID consistently moves to sub-query
                    std::copy(children.begin(), children.end(), std::front_inserter(nodesToProcess));
                } else {
                    std::copy(children.begin(), children.end(), std::back_inserter(nodesToProcess));
                }
            }
        } else if (node->instanceOf<MergeLogicalOperatorNode>()) {

            isFilterAboveAMergeOperator = true;
            std::vector<NodePtr> childrenOfMergeOP = node->getChildren();
            std::copy(childrenOfMergeOP.begin(), childrenOfMergeOP.end(), std::front_inserter(nodesToProcess));
            std::sort(nodesToProcess.begin(),
                      nodesToProcess.end());//To ensure consistency in nodes traversed below a merge operator

        } else if (node->instanceOf<JoinLogicalOperatorNode>()) {
            isFilterAboveAJoinOperator = true;
            //node->as<JoinLogicalOperatorNode>->as<Join::LogicalJoinDefinition>->as<FieldAccessExpressionNode>.
            uint64_t leftSrcId,rightSrcId;
            std::vector<NodePtr> childrenOfJoinOP,childrenLeftOfJoinOP,childrenRightOfJoinOP;
            for (auto& childOfJoinOP : node->getChildren()) {
                if (childOfJoinOP->as<OperatorNode>()->getIsLeftOperator()) {
                    //std::front_inserter(nodesToProcess);
                    //childOfJoinOP->as<OperatorNode>()->getId();
                    for (auto& sourceLeftOfJoinOP : childOfJoinOP->getNodesByType<SourceLogicalOperatorNode>()) {
                    //if(childOfJoinOP->instanceOf<SourceLogicalOperatorNode>()){
                        leftSrcId = sourceLeftOfJoinOP->as<OperatorNode>()->getId();
                    }
                    childrenLeftOfJoinOP.insert(childrenLeftOfJoinOP.end(),childOfJoinOP);
                }else{
                    for (auto& sourceRightOfJoinOP : childOfJoinOP->getNodesByType<SourceLogicalOperatorNode>()) {
                        rightSrcId = sourceRightOfJoinOP->as<OperatorNode>()->getId();
                    }
                    childrenRightOfJoinOP.insert(childrenRightOfJoinOP.end(),childOfJoinOP);
                }
            }

            if(rightSrcId<leftSrcId){
                std::copy(childrenRightOfJoinOP.begin(), childrenRightOfJoinOP.end(), std::back_inserter(nodesToProcess));
            }else{
                std::copy(childrenLeftOfJoinOP.begin(), childrenLeftOfJoinOP.end(), std::back_inserter(nodesToProcess));
            }

        } else if (node->instanceOf<WatermarkAssignerLogicalOperatorNode>()) {

            std::vector<NodePtr> children = node->getChildren();
            WatermarkAssignerLogicalOperatorNodePtr wmLogicalOperatorNodePtr = node->as<WatermarkAssignerLogicalOperatorNode>();
            //const WatermarkStrategyDescriptorPtr wmDescriptor = wmLogicalOperatorNodePtr->getWatermarkStrategyDescriptor();
            std::copy(children.begin(), children.end(), std::back_inserter(nodesToProcess));
        } /*else if (node->instanceOf<WindowLogicalOperatorNode>()) {

            std::vector<NodePtr> children = node->getChildren();
            WindowLogicalOperatorNodePtr windowLogicalOperatorNodePtr = node->as<WindowLogicalOperatorNode>();
            const Windowing::LogicalWindowDefinitionPtr  windowDefinition = windowLogicalOperatorNodePtr->getWindowDefinition();
            std::copy(children.begin(), children.end(), std::back_inserter(nodesToProcess));
        }*/
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
            const FieldAccessExpressionNodePtr accessExpressionNode = (*itr)->as<FieldAccessExpressionNode>();

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
