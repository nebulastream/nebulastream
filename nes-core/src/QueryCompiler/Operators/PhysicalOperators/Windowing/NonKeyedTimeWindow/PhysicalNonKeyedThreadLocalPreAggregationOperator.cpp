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

#include <QueryCompiler/Operators/PhysicalOperators/Windowing/NonKeyedTimeWindow/PhysicalNonKeyedThreadLocalPreAggregationOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/PhysicalWindowOperator.hpp>
#include <Operators/LogicalOperators/Windows/LogicalWindowDefinition.hpp>
#include <Util/Core.hpp>
namespace NES::QueryCompilation::PhysicalOperators {
PhysicalNonKeyedThreadLocalPreAggregationOperator::PhysicalNonKeyedThreadLocalPreAggregationOperator(
    OperatorId id,
    SchemaPtr inputSchema,
    SchemaPtr outputSchema,
    WindowHandlerType keyedEventTimeWindowHandler,
    Windowing::LogicalWindowDefinitionPtr windowDefinition)
    : OperatorNode(id), PhysicalUnaryOperator(id, inputSchema, outputSchema), windowHandler(keyedEventTimeWindowHandler),
      windowDefinition(windowDefinition) {}

std::string PhysicalNonKeyedThreadLocalPreAggregationOperator::toString() const {
    std::stringstream out;
    out << std::endl;
    out << "PhysicalNonKeyedThreadLocalPreAggregationOperator:\n";
    out << PhysicalUnaryOperator::toString();
    out << windowDefinition->toString();
    return out.str();
}

std::shared_ptr<PhysicalOperator>
PhysicalNonKeyedThreadLocalPreAggregationOperator::create(SchemaPtr inputSchema,
                                                          SchemaPtr outputSchema,
                                                          WindowHandlerType keyedEventTimeWindowHandler,
                                                          Windowing::LogicalWindowDefinitionPtr windowDefinition) {
    return std::make_shared<PhysicalNonKeyedThreadLocalPreAggregationOperator>(getNextOperatorId(),
                                                                               inputSchema,
                                                                               outputSchema,
                                                                               keyedEventTimeWindowHandler,
                                                                               windowDefinition);
}

OperatorNodePtr PhysicalNonKeyedThreadLocalPreAggregationOperator::copy() {
    return create(inputSchema, outputSchema, windowHandler, windowDefinition);
}
const Windowing::LogicalWindowDefinitionPtr& PhysicalNonKeyedThreadLocalPreAggregationOperator::getWindowDefinition() const {
    return windowDefinition;
}
}// namespace NES::QueryCompilation::PhysicalOperators