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

#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <QueryCompiler/CodeGenerator.hpp>
#include <QueryCompiler/GeneratableOperators/GeneratableWatermarkAssignerOperator.hpp>
#include <Util/UtilityFunctions.hpp>
#include <Windowing/Watermark/EventTimeWatermarkStrategy.hpp>
#include <Windowing/Watermark/EventTimeWatermarkStrategyDescriptor.hpp>
#include <Windowing/Watermark/ProcessingTimeWatermarkStrategy.hpp>
#include <Windowing/Watermark/ProcessingTimeWatermarkStrategyDescriptor.hpp>

namespace NES {

GeneratableWatermarkAssignerOperatorPtr
GeneratableWatermarkAssignerOperator::create(WatermarkAssignerLogicalOperatorNodePtr watermarkAssignerLogicalOperatorNode,
                                             OperatorId id) {
    auto generatableOperator = std::make_shared<GeneratableWatermarkAssignerOperator>(
        GeneratableWatermarkAssignerOperator(watermarkAssignerLogicalOperatorNode->getWatermarkStrategyDescriptor(), id));
    return generatableOperator;
}

GeneratableWatermarkAssignerOperator::GeneratableWatermarkAssignerOperator(
    const Windowing::WatermarkStrategyDescriptorPtr watermarkStrategyDescriptor, OperatorId id)
    : WatermarkAssignerLogicalOperatorNode(watermarkStrategyDescriptor, id) {}

void GeneratableWatermarkAssignerOperator::produce(CodeGeneratorPtr codegen, PipelineContextPtr context) {
    getChildren()[0]->as<GeneratableOperator>()->produce(codegen, context);
}
void GeneratableWatermarkAssignerOperator::consume(CodeGeneratorPtr codegen, PipelineContextPtr context) {
    auto watermarkStrategyDescriptor = getWatermarkStrategyDescriptor();
    if (auto eventTimeWatermarkStrategyDescriptor =
            std::dynamic_pointer_cast<Windowing::EventTimeWatermarkStrategyDescriptor>(getWatermarkStrategyDescriptor())) {
        auto keyExpression = eventTimeWatermarkStrategyDescriptor->getOnField().getExpressionNode();
        if (!keyExpression->instanceOf<FieldAccessExpressionNode>()) {
            NES_ERROR("GeneratableWatermarkAssignerOperator: watermark field has to be an FieldAccessExpression but it was a "
                      + keyExpression->toString());
        }
        auto fieldAccess = keyExpression->as<FieldAccessExpressionNode>();
        auto watermarkStrategy = Windowing::EventTimeWatermarkStrategy::create(
            fieldAccess, eventTimeWatermarkStrategyDescriptor->getDelay().getTime());
        codegen->generateCodeForWatermarkAssigner(watermarkStrategy, context);
    } else if (auto processingTimeWatermarkStrategyDescriptor =
                   std::dynamic_pointer_cast<Windowing::ProcessingTimeWatermarkStrategyDescriptor>(
                       getWatermarkStrategyDescriptor())) {
        auto watermarkStrategy = Windowing::ProcessingTimeWatermarkStrategy::create();
        codegen->generateCodeForWatermarkAssigner(watermarkStrategy, context);
    } else {
        NES_ERROR("GeneratableWatermarkAssignerOperator: cannot create watermark strategy from descriptor");
    }

    getParents()[0]->as<GeneratableOperator>()->consume(codegen, context);
}
const std::string GeneratableWatermarkAssignerOperator::toString() const {
    std::stringstream ss;
    ss << "GENERATABLE_WATERMARKASSIGNER(" << outputSchema->toString() << ")";
    return ss.str();
}
}// namespace NES