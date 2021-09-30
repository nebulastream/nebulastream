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
#include <Exceptions/TypeInferenceException.hpp>
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <Nodes/Expressions/FieldAssignmentExpressionNode.hpp>
#include <Nodes/Node.hpp>
#include <Nodes/Util/Iterators/DepthFirstNodeIterator.hpp>
#include <Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/JoinLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/MapLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/MergeLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/ProjectionLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/WindowLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/WatermarkAssignerLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Windowing/WindowLogicalOperatorNode.hpp>
#include <Optimizer/QueryRewrite/FilterPushDownRule.hpp>
#include <Optimizer/QueryRewrite/WindowOperatorRules.hpp>
#include <Optimizer/Utils/QuerySignatureUtil.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Windowing/LogicalJoinDefinition.hpp>
#include <Windowing/LogicalWindowDefinition.hpp>
#include <Windowing/Watermark/WatermarkStrategyDescriptor.hpp>
#include <Windowing/WindowingForwardRefs.hpp>
#include <queue>

namespace NES {

WindowOperatorRulesPtr WindowOperatorRules::create() { return std::make_shared<WindowOperatorRules>(WindowOperatorRules()); }

WindowOperatorRules::WindowOperatorRules() {}

QueryPlanPtr WindowOperatorRules::apply(QueryPlanPtr queryPlanPtr) {

    NES_INFO("WindowOperatorRules: Get the Window Operator node in the graph");
    const auto rootOperators = queryPlanPtr->getRootOperators();
    std::vector<WindowLogicalOperatorNodePtr> WindowOperator;

    for (auto rootOperator : rootOperators) {
        //TODO:We only pick one Window
        auto Window = rootOperator->getNodesByType<WindowLogicalOperatorNode>();
        WindowOperator.insert(WindowOperator.end(), Window.begin(), Window.end());
    }


    NES_INFO("WindowOperatorRules: Push the Window operator down in the query plan");
    for (auto oneWindowOperator : WindowOperator) {
        foldWindowAttributes(oneWindowOperator);
    }

    NES_INFO("WindowOperatorRules: Return the updated query plan");
    return queryPlanPtr;
}

void WindowOperatorRules::foldWindowAttributes(WindowLogicalOperatorNodePtr WindowOperator) {

    NES_INFO("WindowOperatorRules: Get children of current Window");
    std::vector<NodePtr> childrenOfWindow = WindowOperator->getChildren();
    NES_INFO("WindowOperatorRules: Copy children to the queue for further processing");
    std::deque<NodePtr> nodesToProcess(childrenOfWindow.begin(), childrenOfWindow.end());

    while (!nodesToProcess.empty()) {

        static bool isWindowAboveAMergeOperator{false};
        static bool isWindowAboveAJoinOperator{false};
        NES_INFO("WindowOperatorRules: Get first operator for processing");
        NodePtr node = nodesToProcess.front();
        nodesToProcess.pop_front();

        if (node->instanceOf<SourceLogicalOperatorNode>())
        {
            NES_INFO("WindowOperatorRules: Window can't be pushed below the " + node->toString() + " operator");

            if (node->as<OperatorNode>()->getId() != WindowOperator->getId()) {

                NES_INFO("WindowOperatorRules: Adding Window operator between current operator and its parents");

                if (isWindowAboveAMergeOperator) {

                    NES_INFO("WindowOperatorRules: Create a duplicate Window operator with new operator ID");
                    OperatorNodePtr duplicatedProjectorOperator = WindowOperator->copy();
                    duplicatedProjectorOperator->setId(UtilityFunctions::getNextOperatorId());

                    if (!(duplicatedProjectorOperator->removeAndJoinParentAndChildren()
                          && node->insertBetweenThisAndParentNodes(duplicatedProjectorOperator))) {

                        NES_ERROR("WindowOperatorRules: Failure in applying Window push down rule");
                        throw std::logic_error("WindowOperatorRules: Failure in applying Window push down rule");
                    }

                    isWindowAboveAMergeOperator = false;
                    continue;
                }

                OperatorNodePtr duplicatedProjectorOperator = WindowOperator->copy();
                duplicatedProjectorOperator->setId(UtilityFunctions::getNextOperatorId());

                if (!(duplicatedProjectorOperator->removeAndJoinParentAndChildren()
                      && node->insertBetweenThisAndParentNodes(duplicatedProjectorOperator))) {

                    NES_ERROR("WindowOperatorRules: Failure in applying Window push down rule");
                    throw std::logic_error("WindowOperatorRules: Failure in applying Window push down rule");
                }
                isWindowAboveAJoinOperator = false; //ToDo::Move this
                continue;
            }
        } else if (node->instanceOf<WindowLogicalOperatorNode>()) {
            auto windowOpDef = node->as<WindowLogicalOperatorNode>()->getWindowDefinition();
            auto windowType = windowOpDef->getWindowType();
            if(windowType.get()->isTumblingWindow()){
                auto window1size = WindowOperator->getWindowDefinition()->getWindowType().get()->getSize();
                auto window2Size = windowType.get()->getSize();
            }

            auto windowAggregation = windowOpDef->getWindowAggregation();
            if(windowOpDef->isKeyed()){
                auto keyField = windowOpDef->getOnKey();
            } //ToDo:Check where it is necessary
            auto windowAggregateFieldName = windowOpDef->getWindowAggregation()->on()->as<FieldAccessExpressionNode>()->getFieldName();
            bool allAttributeFieldsMatch = isFieldsMatchingInWindowAttributes(WindowOperator, windowAggregateFieldName);
            if (window1Size%window2size = 0){
                bool windowSizesAreMultiples = true;
            }
            //if (windowAggregation = Sum())
            if (window2Size%window1size = 0){
                bool windowCanBeRemoved = true;
            }

            if (allAttributeFieldsMatch && windowCanBeRemoved) {
                WindowOperator->removeAndJoinParentAndChildren();
            }
            if (allAttributeFieldsMatch && windowSizesAreMultiples) {
                OperatorNodePtr copyOptr = WindowOperator->duplicate();
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
                } else if (!(copyOptr->removeAndJoinParentAndChildren() && node->insertBetweenThisAndParentNodes(copyOptr) && WindowOperator->removeAndJoinParentAndChildren())) {

                    NES_ERROR("WindowOperatorRules: Failure in applying Window push down rule");
                    throw std::logic_error("WindowOperatorRules: Failure in applying Window push down rule");
                }
                continue;
            } else if (node->instanceOf<MapLogicalOperatorNode>()) {

            //std::vector<std::string> fields;
            //std::sort(fields.begin(), fields.end());
            std::string mapFieldName = getFieldNameUsedByMapOperator(node);
            bool allAttributeFieldsMatch = isFieldsMatchingInWindowAttributes(WindowOperator, mapFieldName);

            if (!allAttributeFieldsMatch) {
                OperatorNodePtr copyOptr = WindowOperator->duplicate();
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
                } else if (!(copyOptr->removeAndJoinParentAndChildren() && node->insertBetweenThisAndParentNodes(copyOptr))) {

                    NES_ERROR("WindowOperatorRules: Failure in applying Window push down rule");
                    throw std::logic_error("WindowOperatorRules: Failure in applying Window push down rule");
                }
                continue;
            } else {
                std::vector<NodePtr> children = node->getChildren();
                if (isWindowAboveAMergeOperator) {//To ensure duplicated filter operator with a new operator ID consistently moves to sub-query
                    std::copy(children.begin(), children.end(), std::front_inserter(nodesToProcess));
                } else {
                    std::copy(children.begin(), children.end(), std::back_inserter(nodesToProcess));
                }
            }
        } else if (node->instanceOf<FilterLogicalOperatorNode>()) {

            isWindowAboveAMergeOperator = true;
            //node->as<OperatorNode>()->getId() != WindowOperator->getId()
            std::vector<NodePtr> childrenOfFilterOP = node->getChildren();
            std::copy(childrenOfFilterOP.begin(), childrenOfFilterOP.end(), std::front_inserter(nodesToProcess));
            std::sort(nodesToProcess.begin(),
                      nodesToProcess.end());//To ensure consistency in nodes traversed below a merge operator

        } else if (node->instanceOf<MergeLogicalOperatorNode>()) {

            isWindowAboveAMergeOperator = true;

            std::vector<NodePtr> childrenOfMergeOP = node->getChildren();
            std::copy(childrenOfMergeOP.begin(), childrenOfMergeOP.end(), std::front_inserter(nodesToProcess));
            std::sort(nodesToProcess.begin(),
                      nodesToProcess.end());//To ensure consistency in nodes traversed below a merge operator

        } else if (node->instanceOf<JoinLogicalOperatorNode>()) {
            isWindowAboveAJoinOperator = true;
            std::vector<NodePtr> childrenOfJoinOP;

            std::string joinKeyFieldName = getFieldNameUsedByJoinOperator(node);
            bool allAttributeFieldsMatch = isFieldsMatchingInWindowAttributes(WindowOperator, joinKeyFieldName);

            if (!allAttributeFieldsMatch) {}
            for (auto& childOfJoinOP : node->getChildren()) {
                childrenOfJoinOP.insert(childrenOfJoinOP.end(),childOfJoinOP);
            }
            std::copy(childrenOfJoinOP.begin(), childrenOfJoinOP.end(), std::back_inserter(nodesToProcess));

        } else if (node->instanceOf<WatermarkAssignerLogicalOperatorNode>()) {

            std::vector<NodePtr> children = node->getChildren();
            WatermarkAssignerLogicalOperatorNodePtr wmLogicalOperatorNodePtr = node->as<WatermarkAssignerLogicalOperatorNode>();
            //const WatermarkStrategyDescriptorPtr wmDescriptor = wmLogicalOperatorNodePtr->getWatermarkStrategyDescriptor();
            std::copy(children.begin(), children.end(), std::back_inserter(nodesToProcess));

        } else {
                std::vector<NodePtr> children = node->getChildren();
                if (isWindowAboveAMergeOperator) {//To ensure duplicated window operator with a new operator ID consistently moves to sub-query
                    std::copy(children.begin(), children.end(), std::front_inserter(nodesToProcess));
                } else {
                    std::copy(children.begin(), children.end(), std::back_inserter(nodesToProcess));
                }
            }
        }
    }
}
//std::vector<std::string> fields;
//std::sort(fields.begin(), fields.end());
bool WindowOperatorRules::isFieldsMatchingInWindowAttributes(WindowLogicalOperatorNodePtr WindowOperator, const std::string fieldName) const {

    NES_INFO("WindowOperatorRules: Create an iterator for traversing the Window attributes");
    std::vector<ExpressionItem> expressions  = WindowOperator->getExpressions();

    NES_INFO("WindowOperatorRules: Iterate and find the predicate with FieldAccessExpression Node");
    for (auto& exp : expressions) {
        auto expression = exp.getExpressionNode();
        if (!expression->instanceOf<FieldAccessExpressionNode>()) {
            NES_ERROR("WindowLogicalOperatorNode: Expression has to be an FieldAccessExpression but it was a "
                      + expression->toString());
            throw TypeInferenceException(
                "WindowLogicalOperatorNode: Expression has to be an FieldAccessExpression but it was a "
                + expression->toString());
        }

        NES_INFO("WindowOperatorRules: Check if the input field name is same as the FieldAccessExpression field name");
        if (expression->as<FieldAccessExpressionNode>()->getFieldName() == fieldName) {
            return true;
        }

        return false;
    }

    return false;
}

std::string WindowOperatorRules::getFieldNameUsedByMapOperator(NodePtr node) const {
    NES_INFO("WindowOperatorRules: Find the field name used in map operator");
    MapLogicalOperatorNodePtr mapLogicalOperatorNodePtr = node->as<MapLogicalOperatorNode>();
    const FieldAssignmentExpressionNodePtr mapExpression = mapLogicalOperatorNodePtr->getMapExpression();
    const FieldAccessExpressionNodePtr field = mapExpression->getField();
    const std::string mapFieldName = field->getFieldName();
    return mapFieldName;
}

std::string WindowOperatorRules::getFieldNameUsedByJoinOperator(NodePtr node) const {
    NES_INFO("WindowOperatorRules: Find the field name used in join operator");
    JoinLogicalOperatorNodePtr joinLogicalOperatorNodePtr = node->as<JoinLogicalOperatorNode>();
    const Join::LogicalJoinDefinitionPtr joinDefinition = joinLogicalOperatorNodePtr->getJoinDefinition();
    const FieldAccessExpressionNodePtr field = joinDefinition->getLeftJoinKey();
    const std::string mapFieldName = field->getFieldName();
    return mapFieldName;
}


}// namespace NES
