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

#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <Operators/Expressions/FieldAssignmentExpressionNode.hpp>
#include <Operators/Expressions/LogicalExpressions/AndExpressionNode.hpp>
#include <Nodes/Iterators/DepthFirstNodeIterator.hpp>
#include <Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/MapLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/ProjectionLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/UnionLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Watermarks/WatermarkAssignerLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Windows/WindowLogicalOperatorNode.hpp>
#include <Optimizer/QuerySignatures/ContainedOperatorsUtil.hpp>
#include <Optimizer/QuerySignatures/QuerySignature.hpp>
#include <Optimizer/QuerySignatures/SignatureContainmentCheck.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <Operators/LogicalOperators/Windows/LogicalWindowDefinition.hpp>
#include <Operators/LogicalOperators/Windows/Measures/TimeCharacteristic.hpp>
#include <Operators/LogicalOperators/Watermarks/EventTimeWatermarkStrategyDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/Aggregations/WindowAggregationDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/Measures/TimeMeasure.hpp>
#include <Operators/LogicalOperators/Windows/Types/TimeBasedWindowType.hpp>
#include <Operators/LogicalOperators/Windows/Types/WindowType.hpp>

namespace NES::Optimizer {

std::vector<LogicalOperatorNodePtr>
ContainedOperatorsUtil::createContainedWindowOperator(const LogicalOperatorNodePtr& containedOperator,
                                                      const LogicalOperatorNodePtr& containerOperator) {
    NES_TRACE("Contained operator: {}", containedOperator->toString());
    std::vector<LogicalOperatorNodePtr> containmentOperators = {};
    auto containedWindowOperators = containedOperator->getNodesByType<WindowLogicalOperatorNode>();
    //obtain the most downstream window operator from the container query plan
    auto containerWindowOperators = containerOperator->getNodesByType<WindowLogicalOperatorNode>().front();
    Windowing::TimeBasedWindowTypePtr containerTimeBasedWindow;
    if (containerWindowOperators->as<WindowLogicalOperatorNode>()
            ->getWindowDefinition()
            ->getWindowType()
            ->isTimeBasedWindowType()) {
        containerTimeBasedWindow =
            containerWindowOperators->as<WindowLogicalOperatorNode>()
                ->getWindowDefinition()
                ->getWindowType()
                ->asTimeBasedWindowType(
                    containerWindowOperators->as<WindowLogicalOperatorNode>()->getWindowDefinition()->getWindowType());
    }
    NES_TRACE("Contained operator: {}", containedOperator->toString());
    if (containerTimeBasedWindow == nullptr) {
        return {};
    }
    //get the correct window operator
    if (!containedWindowOperators.empty()) {
        auto windowOperatorCopy = containedWindowOperators.front()->copy();
        auto watermarkOperatorCopy =
            containedWindowOperators.front()->getChildren()[0]->as<WatermarkAssignerLogicalOperatorNode>()->copy();
        auto windowDefinition = windowOperatorCopy->as<WindowLogicalOperatorNode>()->getWindowDefinition();
        //check that containee is a time based window, else return false
        if (windowDefinition->getWindowType()->isTimeBasedWindowType()) {
            auto timeBasedWindow = windowDefinition->getWindowType()->asTimeBasedWindowType(windowDefinition->getWindowType());
            //we need to set the time characteristic field to start because the previous timestamp will not exist anymore
            auto field = containerOperator->getOutputSchema()->hasFieldName("start");
            //return false if this is not possible
            if (field == nullptr) {
                return {};
            }
            timeBasedWindow->getTimeCharacteristic()->setField(field);
            timeBasedWindow->getTimeCharacteristic()->setTimeUnit(
                containerTimeBasedWindow->getTimeCharacteristic()->getTimeUnit());
            containmentOperators.push_back(windowOperatorCopy->as<LogicalOperatorNode>());
            //obtain the watermark operator
            auto watermarkOperator = watermarkOperatorCopy->as<WatermarkAssignerLogicalOperatorNode>();
            if (watermarkOperator->getWatermarkStrategyDescriptor()
                    ->instanceOf<Windowing::EventTimeWatermarkStrategyDescriptor>()) {
                auto fieldName = field->getName();
                auto containerSourceNames = Util::splitWithStringDelimiter<std::string>(fieldName, "$")[0];
                auto watermarkStrategyDescriptor =
                    watermarkOperator->getWatermarkStrategyDescriptor()->as<Windowing::EventTimeWatermarkStrategyDescriptor>();
                watermarkStrategyDescriptor->setOnField(FieldAccessExpressionNode::create(containerSourceNames + "$start"));
                watermarkStrategyDescriptor->setTimeUnit(containerTimeBasedWindow->getTimeCharacteristic()->getTimeUnit());
            } else {
                return {};
            }
            containmentOperators.push_back(watermarkOperator);
            NES_TRACE("Window containment possible.");
        }
    }
    if (!containmentOperators.empty()
        && !checkDownstreamOperatorChainForSingleParent(
            containedOperator,
            containedWindowOperators.front()->getChildren()[0]->as<LogicalOperatorNode>())) {
        return {};
    }
    containmentOperators.back()->addParent(containmentOperators.front());
    return containmentOperators;
}

LogicalOperatorNodePtr
ContainedOperatorsUtil::createContainedProjectionOperator(const LogicalOperatorNodePtr& containedOperator) {
    auto projectionOperators = containedOperator->getNodesByType<ProjectionLogicalOperatorNode>();
    //get the most downstream projection operator
    if (!projectionOperators.empty()) {
        if (!checkDownstreamOperatorChainForSingleParent(containedOperator, projectionOperators.at(0))) {
            return {};
        }
        return projectionOperators.at(0)->copy()->as<LogicalOperatorNode>();
    }
    return nullptr;
}

LogicalOperatorNodePtr ContainedOperatorsUtil::createContainedFilterOperators(const LogicalOperatorNodePtr& container,
                                                                              const LogicalOperatorNodePtr& containee) {
    NES_DEBUG("Check if filter containment is possible for container {}, containee {}.",
              container->toString(),
              containee->toString());
    //we don't pull up filters from under windows or unions
    if (containee->instanceOf<WindowLogicalOperatorNode>() || containee->instanceOf<UnionLogicalOperatorNode>()) {
        return nullptr;
    }
    //if all checks pass, we extract the filter operators
    std::vector<FilterLogicalOperatorNodePtr> upstreamFilterOperatorsCopy = {};
    std::vector<FilterLogicalOperatorNodePtr> upstreamFilterOperators = containee->getNodesByType<FilterLogicalOperatorNode>();
    for (const auto& filterOperator : upstreamFilterOperators) {
        upstreamFilterOperatorsCopy.push_back(filterOperator->copy()->as<FilterLogicalOperatorNode>());
    }
    try {
        // if there are no upstream filter operators return an empty vector
        if (upstreamFilterOperators.empty()) {
            return nullptr;
            // if there is one upstream filter operator, check the downstream operator chain for single parent relationship
            // and make sure, we are not pulling up from under a map operator that applies a transformation to the predicate
        } else if (upstreamFilterOperators.size() == 1) {
            std::vector<std::string> mapAttributeNames = {};
            if (!checkDownstreamOperatorChainForSingleParentAndMapOperator(containee,
                                                                           upstreamFilterOperators.front(),
                                                                           mapAttributeNames)) {
                return nullptr;
            }
            auto predicate = upstreamFilterOperatorsCopy.front()->getPredicate();
            if (!isMapTransformationAppliedToPredicate(upstreamFilterOperatorsCopy.front(),
                                                       mapAttributeNames,
                                                       container->getOutputSchema())) {
                return nullptr;
            }
            return upstreamFilterOperatorsCopy.front();
            // if there are multiple upstream filter operators, check the downstream operator chain for single parent relationship
            // and make sure, we are not pulling up from under a map operator that applies a transformation to the predicate
            // also concatenate the filter predicates using && and return a new filter with the concatenated predicates
        } else {
            std::vector<std::string> mapAttributeNames = {};
            NES_TRACE("Filter predicate: {}", upstreamFilterOperators.back()->toString());
            if (checkDownstreamOperatorChainForSingleParentAndMapOperator(containee,
                                                                          upstreamFilterOperators.back(),
                                                                          mapAttributeNames)) {
                NES_TRACE("Filter predicate: {}", upstreamFilterOperators.back()->toString());
                auto predicate = upstreamFilterOperatorsCopy.front()->getPredicate();
                if (!isMapTransformationAppliedToPredicate(upstreamFilterOperatorsCopy.front(),
                                                           mapAttributeNames,
                                                           container->getOutputSchema())) {
                    return nullptr;
                }
                for (size_t i = 1; i < upstreamFilterOperatorsCopy.size(); ++i) {
                    if (!isMapTransformationAppliedToPredicate(upstreamFilterOperatorsCopy.at(i),
                                                               mapAttributeNames,
                                                               container->getOutputSchema())) {
                        return nullptr;
                    }
                    predicate = AndExpressionNode::create(predicate, upstreamFilterOperatorsCopy.at(i)->getPredicate());
                }
                NES_DEBUG("Filter predicate: {}", predicate->toString());
                upstreamFilterOperatorsCopy.front()->setPredicate(predicate);
                return upstreamFilterOperatorsCopy.front();
            } else {
                return nullptr;
            }
        }
        return nullptr;
    } catch (const std::exception& e) {
        NES_WARNING("Filter extraction failed: {}", e.what());
        return nullptr;
    }
}

bool ContainedOperatorsUtil::isMapTransformationAppliedToPredicate(FilterLogicalOperatorNodePtr const& filterOperator,
                                                                   const std::vector<std::string>& fieldNames,
                                                                   const SchemaPtr& containerOutputSchema) {

    NES_DEBUG("Create an iterator for traversing the filter {} predicates {}, and check output schema {}",
              filterOperator->toString(),
              filterOperator->getPredicate()->toString(),
              containerOutputSchema->toString());
    const ExpressionNodePtr filterPredicate = filterOperator->getPredicate();
    DepthFirstNodeIterator depthFirstNodeIterator(filterPredicate);
    //traverse the filter predicate
    for (auto itr = depthFirstNodeIterator.begin(); itr != DepthFirstNodeIterator::end(); ++itr) {
        NES_TRACE("Iterate and find the predicate with FieldAccessExpression Node");
        //if the current node is a FieldAccessExpressionNode, check if the field name is still in the container output schema
        if ((*itr)->instanceOf<FieldAccessExpressionNode>()) {
            const FieldAccessExpressionNodePtr accessExpressionNode = (*itr)->as<FieldAccessExpressionNode>();
            NES_TRACE("Is field {} still in container output schema {}? {}",
                      accessExpressionNode->getFieldName(),
                      containerOutputSchema->toString(),
                      containerOutputSchema->contains(accessExpressionNode->getFieldName()));
            //return false if the containerOutputSchema does not contain the field anymore
            if (!containerOutputSchema->contains(accessExpressionNode->getFieldName())) {
                return false;
            }
            NES_TRACE("Check if the input field name is same as the FieldAccessExpression field name");
            return (std::find(fieldNames.begin(), fieldNames.end(), accessExpressionNode->getFieldName()) == fieldNames.end());
        }
        NES_TRACE("New filter predicate: {}", filterPredicate->toString());
    }
    return true;
}

bool ContainedOperatorsUtil::checkDownstreamOperatorChainForSingleParent(
    const LogicalOperatorNodePtr& containedOperator,
    const LogicalOperatorNodePtr& extractedContainedOperator) {
    NES_TRACE("Extracted contained operator: {}", extractedContainedOperator->toString());
    for (const auto& source : containedOperator->getAllLeafNodes()) {
        NodePtr parent = source;
        bool foundExtracted = false;
        //check if the extracted contained operator has only one parent operator
        while (!parent->equal(containedOperator)) {
            if (parent->equal(extractedContainedOperator)) {
                foundExtracted = true;
            }
            //return false if the operator has multiple parents
            if (parent->getParents().size() != 1) {
                return false;
            } else {
                parent = parent->getParents()[0];
            }
            //if we found the extracted contained operator we make sure that none of its parents are unions or windows
            //we cannot pull up from under a union or a window operator
            if (foundExtracted) {
                if (parent->instanceOf<UnionLogicalOperatorNode>()
                    || (extractedContainedOperator->instanceOf<ProjectionLogicalOperatorNode>()
                        && parent->instanceOf<WindowLogicalOperatorNode>())) {
                    return false;
                }
            }
        }
    }
    return true;
}

bool ContainedOperatorsUtil::checkDownstreamOperatorChainForSingleParentAndMapOperator(
    const LogicalOperatorNodePtr& containedOperator,
    const LogicalOperatorNodePtr& extractedContainedOperator,
    std::vector<std::string>& mapAttributeNames) {
    NES_DEBUG("extractedContainedOperator parents size: {}", extractedContainedOperator->getParents().size());
    //return false if the extracted contained operator has multiple parents
    if (extractedContainedOperator->hasMultipleParents()) {
        return false;
    }
    for (const auto& source : containedOperator->getAllLeafNodes()) {
        NodePtr parent = source;
        NES_DEBUG("Parent: {}", parent->toString());
        bool foundExtracted = false;
        while (!parent->equal(containedOperator)) {
            NES_DEBUG("Parent: {}", parent->toString());
            if (parent->equal(extractedContainedOperator)) {
                foundExtracted = true;
            }
            if (foundExtracted) {
                //we don't pull up filter operators from under union operators
                if (parent->instanceOf<WindowLogicalOperatorNode>() || parent->instanceOf<UnionLogicalOperatorNode>()) {
                    return false;
                }
            }
            //we cannot guarantee correct results if the operator has multiple parents when merging
            //therefore, we don't allow that
            if (parent->getParents().size() != 1) {
                return false;
            } else {
                //if there is only one parent, we check if the current operator is a map operator and add its field name to the mapAttributeNames vector
                if (foundExtracted) {
                    if (parent->instanceOf<MapLogicalOperatorNode>()) {
                        auto mapOperator = parent->as<MapLogicalOperatorNode>();
                        mapAttributeNames.push_back(mapOperator->getMapExpression()->getField()->getFieldName());
                    }
                }
                parent = parent->getParents()[0];
            }
        }
    }
    return true;
}
}// namespace NES::Optimizer