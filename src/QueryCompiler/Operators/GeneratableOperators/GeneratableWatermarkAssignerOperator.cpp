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
#include <QueryCompiler/CodeGenerator/CodeGenerator.hpp>
#include <QueryCompiler/Operators/GeneratableOperators/GeneratableWatermarkAssignmentOperator.hpp>
#include <Util/UtilityFunctions.hpp>
#include <Windowing/Watermark/EventTimeWatermarkStrategy.hpp>
#include <Windowing/Watermark/EventTimeWatermarkStrategyDescriptor.hpp>
#include <Windowing/Watermark/IngestionTimeWatermarkStrategy.hpp>
#include <Windowing/Watermark/IngestionTimeWatermarkStrategyDescriptor.hpp>
#include <utility>

namespace NES::QueryCompilation::GeneratableOperators {

GeneratableOperatorPtr
GeneratableWatermarkAssignmentOperator::create(SchemaPtr inputSchema,
                                               SchemaPtr outputSchema,
                                               Windowing::WatermarkStrategyDescriptorPtr watermarkStrategyDescriptor) {
    return create(Util::getNextOperatorId(),
                  std::move(inputSchema),
                  std::move(outputSchema),
                  std::move(watermarkStrategyDescriptor));
}

GeneratableOperatorPtr
GeneratableWatermarkAssignmentOperator::create(OperatorId id,
                                               SchemaPtr inputSchema,
                                               SchemaPtr outputSchema,
                                               Windowing::WatermarkStrategyDescriptorPtr watermarkStrategyDescriptor) {
    return std::make_shared<GeneratableWatermarkAssignmentOperator>(
        GeneratableWatermarkAssignmentOperator(id,
                                               std::move(inputSchema),
                                               std::move(outputSchema),
                                               std::move(watermarkStrategyDescriptor)));
}

GeneratableWatermarkAssignmentOperator::GeneratableWatermarkAssignmentOperator(
    OperatorId id,
    SchemaPtr inputSchema,
    SchemaPtr outputSchema,
    Windowing::WatermarkStrategyDescriptorPtr watermarkStrategyDescriptor)
    : OperatorNode(id), GeneratableOperator(id, std::move(inputSchema), std::move(outputSchema)),
      watermarkStrategyDescriptor(std::move(watermarkStrategyDescriptor)) {}

void GeneratableWatermarkAssignmentOperator::generateExecute(CodeGeneratorPtr codegen, PipelineContextPtr context) {
    if (auto eventTimeWatermarkStrategyDescriptor =
            std::dynamic_pointer_cast<Windowing::EventTimeWatermarkStrategyDescriptor>(watermarkStrategyDescriptor)) {
        auto keyExpression = eventTimeWatermarkStrategyDescriptor->getOnField().getExpressionNode();
        if (!keyExpression->instanceOf<FieldAccessExpressionNode>()) {
            NES_ERROR("GeneratableWatermarkAssignerOperator: watermark field has to be an FieldAccessExpression but it was a "
                      + keyExpression->toString());
        }
        auto fieldAccess = keyExpression->as<FieldAccessExpressionNode>();
        auto watermarkStrategy =
            Windowing::EventTimeWatermarkStrategy::create(fieldAccess,
                                                          eventTimeWatermarkStrategyDescriptor->getAllowedLateness().getTime(),
                                                          eventTimeWatermarkStrategyDescriptor->getTimeUnit().getMultiplier());
        codegen->generateCodeForWatermarkAssigner(watermarkStrategy, context);
    } else if (std::dynamic_pointer_cast<Windowing::IngestionTimeWatermarkStrategyDescriptor>(watermarkStrategyDescriptor)) {
        auto watermarkStrategy = Windowing::IngestionTimeWatermarkStrategy::create();
        codegen->generateCodeForWatermarkAssigner(watermarkStrategy, context);
    } else {
        NES_ERROR("GeneratableWatermarkAssignerOperator: cannot create watermark strategy from descriptor");
    }
}

std::string GeneratableWatermarkAssignmentOperator::toString() const { return "GeneratableWatermarkAssignmentOperator"; }

OperatorNodePtr GeneratableWatermarkAssignmentOperator::copy() {
    return create(id, inputSchema, outputSchema, watermarkStrategyDescriptor);
}

}// namespace NES::QueryCompilation::GeneratableOperators