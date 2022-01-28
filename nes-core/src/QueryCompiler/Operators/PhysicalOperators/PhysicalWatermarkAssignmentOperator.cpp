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
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalWatermarkAssignmentOperator.hpp>
#include <utility>

namespace NES::QueryCompilation::PhysicalOperators {

PhysicalWatermarkAssignmentOperator::PhysicalWatermarkAssignmentOperator(
    OperatorId id,
    SchemaPtr inputSchema,
    SchemaPtr outputSchema,
    Windowing::WatermarkStrategyDescriptorPtr watermarkStrategyDescriptor)
    : OperatorNode(id), PhysicalUnaryOperator(id, std::move(inputSchema), std::move(outputSchema)),
      watermarkStrategyDescriptor(std::move(watermarkStrategyDescriptor)) {}

PhysicalOperatorPtr
PhysicalWatermarkAssignmentOperator::create(OperatorId id,
                                            const SchemaPtr& inputSchema,
                                            const SchemaPtr& outputSchema,
                                            const Windowing::WatermarkStrategyDescriptorPtr& watermarkStrategyDescriptor) {
    return std::make_shared<PhysicalWatermarkAssignmentOperator>(id, inputSchema, outputSchema, watermarkStrategyDescriptor);
}

Windowing::WatermarkStrategyDescriptorPtr PhysicalWatermarkAssignmentOperator::getWatermarkStrategyDescriptor() const {
    return watermarkStrategyDescriptor;
}

PhysicalOperatorPtr
PhysicalWatermarkAssignmentOperator::create(SchemaPtr inputSchema,
                                            SchemaPtr outputSchema,
                                            Windowing::WatermarkStrategyDescriptorPtr watermarkStrategyDescriptor) {
    return create(Util::getNextOperatorId(),
                  std::move(inputSchema),
                  std::move(outputSchema),
                  std::move(watermarkStrategyDescriptor));
}

std::string PhysicalWatermarkAssignmentOperator::toString() const { return "PhysicalWatermarkAssignmentOperator"; }

OperatorNodePtr PhysicalWatermarkAssignmentOperator::copy() {
    return create(id, inputSchema, outputSchema, getWatermarkStrategyDescriptor());
}

}// namespace NES::QueryCompilation::PhysicalOperators