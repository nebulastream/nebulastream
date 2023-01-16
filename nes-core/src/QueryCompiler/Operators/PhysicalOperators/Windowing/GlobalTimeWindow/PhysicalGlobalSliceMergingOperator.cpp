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

#include <QueryCompiler/Operators/PhysicalOperators/Windowing/GlobalTimeWindow/PhysicalGlobalSliceMergingOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/PhysicalWindowOperator.hpp>
#include <memory>
namespace NES::QueryCompilation::PhysicalOperators {

PhysicalGlobalSliceMergingOperator::PhysicalGlobalSliceMergingOperator(OperatorId id,
                                                                       SchemaPtr inputSchema,
                                                                       SchemaPtr outputSchema,
                                                                       WindowHandlerType operatorHandler,
                                                                       Windowing::LogicalWindowDefinitionPtr windowDefinition)
    : OperatorNode(id), PhysicalUnaryOperator(id, inputSchema, outputSchema), AbstractScanOperator(),
      operatorHandler(operatorHandler), windowDefinition(windowDefinition) {}
std::string PhysicalGlobalSliceMergingOperator::toString() const { return "PhysicalGlobalSliceMergingOperator"; }

std::shared_ptr<PhysicalGlobalSliceMergingOperator>
PhysicalGlobalSliceMergingOperator::create(SchemaPtr inputSchema,
                                           SchemaPtr outputSchema,
                                           WindowHandlerType keyedEventTimeWindowHandler,
                                           Windowing::LogicalWindowDefinitionPtr windowDefinition) {
    return std::make_shared<PhysicalGlobalSliceMergingOperator>(Util::getNextOperatorId(),
                                                                inputSchema,
                                                                outputSchema,
                                                                keyedEventTimeWindowHandler,
                                                                windowDefinition);
}

PhysicalGlobalSliceMergingOperator::WindowHandlerType PhysicalGlobalSliceMergingOperator::getWindowHandler() {
    return operatorHandler;
}

OperatorNodePtr PhysicalGlobalSliceMergingOperator::copy() {
    return create(inputSchema, outputSchema, operatorHandler, windowDefinition);
}
const Windowing::LogicalWindowDefinitionPtr& PhysicalGlobalSliceMergingOperator::getWindowDefinition() const {
    return windowDefinition;
}

}// namespace NES::QueryCompilation::PhysicalOperators