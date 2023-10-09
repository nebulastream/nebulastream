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

#include <QueryCompiler/Operators/PhysicalOperators/Windowing/KeyedTimeWindow/PhysicalKeyedThreadLocalPreAggregationOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/PhysicalWindowOperator.hpp>
#include <Util/Core.hpp>
#include <utility>
namespace NES {
namespace QueryCompilation {
namespace PhysicalOperators {

PhysicalKeyedThreadLocalPreAggregationOperator::PhysicalKeyedThreadLocalPreAggregationOperator(
    OperatorId id,
    SchemaPtr inputSchema,
    SchemaPtr outputSchema,
    WindowHandlerType keyedEventTimeWindowHandler,
    Windowing::LogicalWindowDefinitionPtr windowDefinition)
    : OperatorNode(id), PhysicalUnaryOperator(id, std::move(inputSchema), std::move(outputSchema)),
      keyedEventTimeWindowHandler(std::move(keyedEventTimeWindowHandler)), windowDefinition(windowDefinition) {}

std::string PhysicalKeyedThreadLocalPreAggregationOperator::toString() const {
    return "PhysicalKeyedThreadLocalPreAggregationOperator";
}

std::shared_ptr<PhysicalOperator>
PhysicalKeyedThreadLocalPreAggregationOperator::create(const SchemaPtr& inputSchema,
                                                       const SchemaPtr& outputSchema,
                                                       const WindowHandlerType& keyedEventTimeWindowHandler,
                                                       const Windowing::LogicalWindowDefinitionPtr& windowDefinition) {
    return std::make_shared<PhysicalKeyedThreadLocalPreAggregationOperator>(getNextOperatorId(),
                                                                            inputSchema,
                                                                            outputSchema,
                                                                            keyedEventTimeWindowHandler,
                                                                            windowDefinition);
}

const Windowing::LogicalWindowDefinitionPtr& PhysicalKeyedThreadLocalPreAggregationOperator::getWindowDefinition() const {
    return windowDefinition;
}

PhysicalKeyedThreadLocalPreAggregationOperator::WindowHandlerType
PhysicalKeyedThreadLocalPreAggregationOperator::getWindowHandler() {
    return keyedEventTimeWindowHandler;
}

OperatorNodePtr PhysicalKeyedThreadLocalPreAggregationOperator::copy() {
    return create(inputSchema, outputSchema, keyedEventTimeWindowHandler, windowDefinition);
}
}// namespace PhysicalOperators
}// namespace QueryCompilation
}// namespace NES