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


#include <Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/MapLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/MergeLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Windowing/CentralWindowOperator.hpp>
#include <Operators/LogicalOperators/Windowing/SliceCreationOperator.hpp>
#include <Operators/LogicalOperators/Windowing/SliceMergingOperator.hpp>
#include <Operators/LogicalOperators/Windowing/WindowComputationOperator.hpp>
#include <Operators/LogicalOperators/Windowing/WindowLogicalOperatorNode.hpp>

#include <QueryCompiler/GeneratableOperators/Windowing/GeneratableCombiningWindowOperator.hpp>
#include <QueryCompiler/GeneratableOperators/Windowing/GeneratableCompleteWindowOperator.hpp>
#include <QueryCompiler/GeneratableOperators/Windowing/GeneratableSlicingWindowOperator.hpp>
#include <QueryCompiler/GeneratableOperators/GeneratableWatermarkAssignerOperator.hpp>

#include <QueryCompiler/GeneratableOperators/Windowing/Aggregations/GeneratableCountAggregation.hpp>
#include <QueryCompiler/GeneratableOperators/Windowing/Aggregations/GeneratableMaxAggregation.hpp>
#include <QueryCompiler/GeneratableOperators/Windowing/Aggregations/GeneratableMinAggregation.hpp>
#include <QueryCompiler/GeneratableOperators/Windowing/Aggregations/GeneratableSumAggregation.hpp>

#include <Windowing/LogicalWindowDefinition.hpp>
#include <Windowing/LogicalJoinDefinition.hpp>
#include <Operators/LogicalOperators/JoinLogicalOperatorNode.hpp>

#include <Windowing/WindowAggregations/WindowAggregationDescriptor.hpp>

#include <QueryCompiler/GeneratableOperators/GeneratableFilterOperator.hpp>
#include <QueryCompiler/GeneratableOperators/GeneratableMapOperator.hpp>
#include <QueryCompiler/GeneratableOperators/GeneratableJoinOperator.hpp>
#include <QueryCompiler/GeneratableOperators/GeneratableMergeOperator.hpp>
#include <QueryCompiler/GeneratableOperators/GeneratableScanOperator.hpp>
#include <QueryCompiler/GeneratableOperators/GeneratableSinkOperator.hpp>
#include <QueryCompiler/GeneratableOperators/TranslateToGeneratableOperatorPhase.hpp>

namespace NES {

TranslateToGeneratableOperatorPhasePtr TranslateToGeneratableOperatorPhase::create() {
    return std::shared_ptr<TranslateToGeneratableOperatorPhase>();
}

TranslateToGeneratableOperatorPhase::TranslateToGeneratableOperatorPhase() = default;

OperatorNodePtr TranslateToGeneratableOperatorPhase::transformIndividualOperator(OperatorNodePtr operatorNode, OperatorNodePtr generatableParentOperator) {

    if (operatorNode->instanceOf<SourceLogicalOperatorNode>()) {
        // Translate Source operator node.
        auto scanOperator = operatorNode->as<SourceLogicalOperatorNode>();
        auto childOperator = GeneratableScanOperator::create(scanOperator->getOutputSchema());
        generatableParentOperator->addChild(childOperator);
        return childOperator;
    } else if (operatorNode->instanceOf<FilterLogicalOperatorNode>()) {
        auto childOperator = GeneratableFilterOperator::create(operatorNode->as<FilterLogicalOperatorNode>());
        generatableParentOperator->addChild(childOperator);
        return childOperator;
    } else if (operatorNode->instanceOf<MapLogicalOperatorNode>()) {
        auto childOperator = GeneratableMapOperator::create(operatorNode->as<MapLogicalOperatorNode>());
        generatableParentOperator->addChild(childOperator);
        return childOperator;
    } else if (operatorNode->instanceOf<MergeLogicalOperatorNode>()) {
        auto scanOperator = GeneratableScanOperator::create(operatorNode->getOutputSchema());
        generatableParentOperator->addChild(scanOperator);
        auto childOperator = GeneratableMergeOperator::create(operatorNode->as<MergeLogicalOperatorNode>());
        scanOperator->addChild(childOperator);
        return childOperator;
    } else if (operatorNode->instanceOf<SinkLogicalOperatorNode>()) {
        return GeneratableSinkOperator::create(operatorNode->as<SinkLogicalOperatorNode>());
    } else if (operatorNode->instanceOf<WindowOperatorNode>()) {
        return TranslateToGeneratableOperatorPhase::transformWindowOperator(operatorNode->as<WindowOperatorNode>(), generatableParentOperator);
    } else if (operatorNode->instanceOf<JoinLogicalOperatorNode>()) {
        auto scanOperator = GeneratableScanOperator::create(operatorNode->getOutputSchema());
        generatableParentOperator->addChild(scanOperator);
        auto childOperator = GeneratableJoinOperator::create(operatorNode->as<JoinLogicalOperatorNode>());
        scanOperator->addChild(childOperator);
        return childOperator;
    } else {
        NES_FATAL_ERROR("TranslateToGeneratableOperatorPhase: No transformation implemented for this operator node: " << operatorNode);
        NES_NOT_IMPLEMENTED();
    }
    } else if (operatorNode->instanceOf<WatermarkAssignerLogicalOperatorNode>()) {
        auto watermarkAssignerOperator = GeneratableWatermarkAssignerOperator::create(operatorNode->as<WatermarkAssignerLogicalOperatorNode>());
        generatableParentOperator->addChild(watermarkAssignerOperator);
        return watermarkAssignerOperator;
    }
    NES_FATAL_ERROR("TranslateToGeneratableOperatorPhase: No transformation implemented for this operator node: " << operatorNode);
    NES_NOT_IMPLEMENTED();
}

OperatorNodePtr TranslateToGeneratableOperatorPhase::transformWindowOperator(WindowOperatorNodePtr windowOperator, OperatorNodePtr generatableParentOperator) {
    auto windowDefinition = windowOperator->getWindowDefinition();
    auto generatableWindowAggregation = transformWindowAggregation(windowDefinition->getWindowAggregation());
    auto scanOperator = GeneratableScanOperator::create(windowOperator->getOutputSchema());
    generatableParentOperator->addChild(scanOperator);
    if (windowOperator->instanceOf<CentralWindowOperator>()) {
        auto generatableWindowOperator = GeneratableCompleteWindowOperator::create(windowDefinition, generatableWindowAggregation);
        scanOperator->addChild(generatableWindowOperator);
        return generatableWindowOperator;
    } else if (windowOperator->instanceOf<SliceCreationOperator>()) {
        auto generatableWindowOperator = GeneratableSlicingWindowOperator::create(windowDefinition, generatableWindowAggregation);
        scanOperator->addChild(generatableWindowOperator);
        return generatableWindowOperator;
    } else if (windowOperator->instanceOf<WindowComputationOperator>()) {
        auto generatableWindowOperator = GeneratableCombiningWindowOperator::create(windowDefinition, generatableWindowAggregation);
        scanOperator->addChild(generatableWindowOperator);
        return generatableWindowOperator;
    }
    NES_FATAL_ERROR("TranslateToGeneratableOperatorPhase: No transformation implemented for this operator node: " << windowOperator);
    NES_NOT_IMPLEMENTED();
}


GeneratableWindowAggregationPtr TranslateToGeneratableOperatorPhase::transformWindowAggregation(Windowing::WindowAggregationDescriptorPtr windowAggregationDescriptor) {
    switch (windowAggregationDescriptor->getType()) {
        case Windowing::WindowAggregationDescriptor::Count: {
            return GeneratableCountAggregation::create(windowAggregationDescriptor);
        };
        case Windowing::WindowAggregationDescriptor::Max: {
            return GeneratableMaxAggregation::create(windowAggregationDescriptor);
        };
        case Windowing::WindowAggregationDescriptor::Min: {
            return GeneratableMinAggregation::create(windowAggregationDescriptor);
        };
        case Windowing::WindowAggregationDescriptor::Sum: {
            return GeneratableSumAggregation::create(windowAggregationDescriptor);
        };
        default:
            NES_FATAL_ERROR("TranslateToGeneratableOperatorPhase: No transformation implemented for this window aggregation type: " << windowAggregationDescriptor->getType());
            NES_NOT_IMPLEMENTED();
    }
}

OperatorNodePtr TranslateToGeneratableOperatorPhase::transform(OperatorNodePtr operatorNode, OperatorNodePtr legacyParent) {
    NES_DEBUG("TranslateToGeneratableOperatorPhase: translate " << operatorNode);
    auto legacyOperator = transformIndividualOperator(operatorNode, legacyParent);
    for (const NodePtr& child : operatorNode->getChildren()) {
        auto generatableOperator = transform(child->as<OperatorNode>(), legacyOperator);
    }
    NES_DEBUG("TranslateToGeneratableOperatorPhase: got " << legacyOperator);
    return legacyOperator;
}

}// namespace NES