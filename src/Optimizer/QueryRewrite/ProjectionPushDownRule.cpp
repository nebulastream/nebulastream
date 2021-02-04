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
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/WatermarkAssignerLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Windowing/WindowLogicalOperatorNode.hpp>
#include <Optimizer/QueryRewrite/FilterPushDownRule.hpp>
#include <Optimizer/QueryRewrite/ProjectionPushDownRule.hpp>
#include <Optimizer/Utils/QuerySignatureUtil.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Windowing/LogicalJoinDefinition.hpp>
#include <Windowing/LogicalWindowDefinition.hpp>
#include <Windowing/Watermark/WatermarkStrategyDescriptor.hpp>
#include <Windowing/WindowingForwardRefs.hpp>
#include <queue>

namespace NES {

ProjectionPushDownRulePtr ProjectionPushDownRule::create() { return std::make_shared<ProjectionPushDownRule>(ProjectionPushDownRule()); }

ProjectionPushDownRule::ProjectionPushDownRule() {}

QueryPlanPtr ProjectionPushDownRule::apply(QueryPlanPtr queryPlanPtr) {

    NES_INFO("ProjectionPushDownRule: Get the Projection node in the graph");
    const auto rootOperators = queryPlanPtr->getRootOperators();
    std::vector<ProjectionLogicalOperatorNodePtr> projectionOperator;

    for (auto rootOperator : rootOperators) {
        //TODO:We only pick one projection
        auto projection = rootOperator->getNodesByType<ProjectionLogicalOperatorNode>();
        projectionOperator.insert(projectionOperator.end(), projection.begin(), projection.end());
    }


    NES_INFO("ProjectionPushDownRule: Push the projection operator down in the query plan");
    for (auto oneProjectionOperator : projectionOperator) {
        pushDownProjection(oneProjectionOperator);
    }

    NES_INFO("ProjectionPushDownRule: Return the updated query plan");
    return queryPlanPtr;
}

void ProjectionPushDownRule::pushDownProjection(ProjectionLogicalOperatorNodePtr projectionOperator) {

    NES_INFO("ProjectionPushDownRule: Get children of current projection");
    std::vector<NodePtr> childrenOfProjection = projectionOperator->getChildren();
    NES_INFO("ProjectionPushDownRule: Copy children to the queue for further processing");
    std::deque<NodePtr> nodesToProcess(childrenOfProjection.begin(), childrenOfProjection.end());

    while (!nodesToProcess.empty()) {

        static bool isProjectionAboveAMergeOperator{false};
        static bool isProjectionAboveAJoinOperator{false};
        NES_INFO("ProjectionPushDownRule: Get first operator for processing");
        NodePtr node = nodesToProcess.front();
        nodesToProcess.pop_front();

        if (node->instanceOf<SourceLogicalOperatorNode>()|| node->instanceOf<ProjectionLogicalOperatorNode>())
        {
            NES_INFO("ProjectionPushDownRule: projection can't be pushed below the " + node->toString() + " operator");

            if (node->as<OperatorNode>()->getId() != projectionOperator->getId()) {

                NES_INFO("ProjectionPushDownRule: Adding projection operator between current operator and its parents");

                if (isProjectionAboveAMergeOperator) {

                    NES_INFO("ProjectionPushDownRule: Create a duplicate projection operator with new operator ID");
                    OperatorNodePtr duplicatedProjectorOperator = projectionOperator->copy();
                    duplicatedProjectorOperator->setId(UtilityFunctions::getNextOperatorId());

                    if (!(duplicatedProjectorOperator->removeAndJoinParentAndChildren()
                          && node->insertBetweenThisAndParentNodes(duplicatedProjectorOperator))) {

                        NES_ERROR("ProjectionPushDownRule: Failure in applying projection push down rule");
                        throw std::logic_error("ProjectionPushDownRule: Failure in applying projection push down rule");
                    }

                    isProjectionAboveAMergeOperator = false;
                    continue;
                }

                OperatorNodePtr duplicatedProjectorOperator = projectionOperator->copy();
                duplicatedProjectorOperator->setId(UtilityFunctions::getNextOperatorId());

                if (!(duplicatedProjectorOperator->removeAndJoinParentAndChildren()
                      && node->insertBetweenThisAndParentNodes(duplicatedProjectorOperator))) {

                    NES_ERROR("ProjectionPushDownRule: Failure in applying projection push down rule");
                    throw std::logic_error("ProjectionPushDownRule: Failure in applying projection push down rule");
                }
                isProjectionAboveAJoinOperator = false; //ToDo::Move this
                continue;
            }
        } else if (node->instanceOf<MapLogicalOperatorNode>()) {

            //std::vector<std::string> fields;
            //std::sort(fields.begin(), fields.end());
            std::string mapFieldName = getFieldNameUsedByMapOperator(node);
            bool allAttributeFieldsMatch = isFieldsMatchingInProjectionAttributes(projectionOperator, mapFieldName);

            if (!allAttributeFieldsMatch) {
                OperatorNodePtr copyOptr = projectionOperator->duplicate();
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

                    NES_ERROR("ProjectionPushDownRule: Failure in applying projection push down rule");
                    throw std::logic_error("ProjectionPushDownRule: Failure in applying projection push down rule");
                }
                continue;
            } else {
                std::vector<NodePtr> children = node->getChildren();
                if (isProjectionAboveAMergeOperator) {//To ensure duplicated filter operator with a new operator ID consistently moves to sub-query
                    std::copy(children.begin(), children.end(), std::front_inserter(nodesToProcess));
                } else {
                    std::copy(children.begin(), children.end(), std::back_inserter(nodesToProcess));
                }
            }
        } else if (node->instanceOf<FilterLogicalOperatorNode>()) {

            isProjectionAboveAMergeOperator = true;
            //node->as<OperatorNode>()->getId() != projectionOperator->getId()
            std::vector<NodePtr> childrenOfFilterOP = node->getChildren();
            std::copy(childrenOfFilterOP.begin(), childrenOfFilterOP.end(), std::front_inserter(nodesToProcess));
            std::sort(nodesToProcess.begin(),
                      nodesToProcess.end());//To ensure consistency in nodes traversed below a merge operator

        } else if (node->instanceOf<MergeLogicalOperatorNode>()) {

            isProjectionAboveAMergeOperator = true;

            std::vector<NodePtr> childrenOfMergeOP = node->getChildren();
            std::copy(childrenOfMergeOP.begin(), childrenOfMergeOP.end(), std::front_inserter(nodesToProcess));
            std::sort(nodesToProcess.begin(),
                      nodesToProcess.end());//To ensure consistency in nodes traversed below a merge operator

        } else if (node->instanceOf<JoinLogicalOperatorNode>()) {
            isProjectionAboveAJoinOperator = true;
            std::vector<NodePtr> childrenOfJoinOP;

            std::string joinKeyFieldName = getFieldNameUsedByJoinOperator(node);
            bool allAttributeFieldsMatch = isFieldsMatchingInProjectionAttributes(projectionOperator, joinKeyFieldName);

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

        } else if (node->instanceOf<WindowLogicalOperatorNode>()) {
            auto windowOpDef = node->as<WindowLogicalOperatorNode>()->getWindowDefinition();
            if(windowOpDef->isKeyed()){
                auto keyField = windowOpDef->getOnKey();
            } //ToDo:Check where it is necessary
            auto windowAggregateFieldName = windowOpDef->getWindowAggregation()->on()->as<FieldAccessExpressionNode>()->getFieldName();
            bool allAttributeFieldsMatch = isFieldsMatchingInProjectionAttributes(projectionOperator, windowAggregateFieldName);

            if (!allAttributeFieldsMatch) {
                OperatorNodePtr copyOptr = projectionOperator->duplicate();
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

                    NES_ERROR("ProjectionPushDownRule: Failure in applying projection push down rule");
                    throw std::logic_error("ProjectionPushDownRule: Failure in applying projection push down rule");
                }
                continue;
            } else {
                std::vector<NodePtr> children = node->getChildren();
                if (isProjectionAboveAMergeOperator) {//To ensure duplicated filter operator with a new operator ID consistently moves to sub-query
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
bool ProjectionPushDownRule::isFieldsMatchingInProjectionAttributes(ProjectionLogicalOperatorNodePtr projectionOperator, const std::string fieldName) const {

    NES_INFO("ProjectionPushDownRule: Create an iterator for traversing the projection attributes");
    std::vector<ExpressionItem> expressions  = projectionOperator->getExpressions();

    NES_INFO("ProjectionPushDownRule: Iterate and find the predicate with FieldAccessExpression Node");
    for (auto& exp : expressions) {
        auto expression = exp.getExpressionNode();
        if (!expression->instanceOf<FieldAccessExpressionNode>()) {
            NES_ERROR("ProjectionLogicalOperatorNode: Expression has to be an FieldAccessExpression but it was a "
                      + expression->toString());
            throw TypeInferenceException(
                "ProjectionLogicalOperatorNode: Expression has to be an FieldAccessExpression but it was a "
                + expression->toString());
        }

        NES_INFO("ProjectionPushDownRule: Check if the input field name is same as the FieldAccessExpression field name");
        if (expression->as<FieldAccessExpressionNode>()->getFieldName() == fieldName) {
            return true;
        }

        return false;
    }

    return false;
}

std::string ProjectionPushDownRule::getFieldNameUsedByMapOperator(NodePtr node) const {
    NES_INFO("ProjectionPushDownRule: Find the field name used in map operator");
    MapLogicalOperatorNodePtr mapLogicalOperatorNodePtr = node->as<MapLogicalOperatorNode>();
    const FieldAssignmentExpressionNodePtr mapExpression = mapLogicalOperatorNodePtr->getMapExpression();
    const FieldAccessExpressionNodePtr field = mapExpression->getField();
    const std::string mapFieldName = field->getFieldName();
    return mapFieldName;
}

std::string ProjectionPushDownRule::getFieldNameUsedByJoinOperator(NodePtr node) const {
    NES_INFO("ProjectionPushDownRule: Find the field name used in join operator");
    JoinLogicalOperatorNodePtr joinLogicalOperatorNodePtr = node->as<JoinLogicalOperatorNode>();
    const Join::LogicalJoinDefinitionPtr joinDefinition = joinLogicalOperatorNodePtr->getJoinDefinition();
    const FieldAccessExpressionNodePtr field = joinDefinition->getLeftJoinKey();
    const std::string mapFieldName = field->getFieldName();
    return mapFieldName;
}


}// namespace NES
