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

#include <QueryCompiler/Operators/PhysicalOperators/Windowing/NonKeyedTimeWindow/PhysicalNonKeyedSlidingWindowSink.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/PhysicalWindowOperator.hpp>
#include <memory>

namespace NES::QueryCompilation::PhysicalOperators {

PhysicalNonKeyedSlidingWindowSink::PhysicalNonKeyedSlidingWindowSink(
    OperatorId id,
    SchemaPtr inputSchema,
    SchemaPtr outputSchema,
    Windowing::Experimental::NonKeyedSlidingWindowSinkOperatorHandlerPtr keyedEventTimeWindowHandler,
    Windowing::LogicalWindowDefinitionPtr windowDefinition)
    : OperatorNode(id), PhysicalUnaryOperator(id, inputSchema, outputSchema), AbstractScanOperator(),
      keyedEventTimeWindowHandler(keyedEventTimeWindowHandler), windowDefinition(windowDefinition) {}

std::shared_ptr<PhysicalNonKeyedSlidingWindowSink> PhysicalNonKeyedSlidingWindowSink::create(
    SchemaPtr inputSchema,
    SchemaPtr outputSchema,
    Windowing::Experimental::NonKeyedSlidingWindowSinkOperatorHandlerPtr keyedEventTimeWindowHandler,
    Windowing::LogicalWindowDefinitionPtr windowDefinition) {
    return std::make_shared<PhysicalNonKeyedSlidingWindowSink>(getNextOperatorId(),
                                                               inputSchema,
                                                               outputSchema,
                                                               keyedEventTimeWindowHandler,
                                                               windowDefinition);
}

Windowing::LogicalWindowDefinitionPtr PhysicalNonKeyedSlidingWindowSink::getWindowDefinition() { return windowDefinition; }

std::string PhysicalNonKeyedSlidingWindowSink::toString() const { return "PhysicalNonKeyedSlidingWindowSink"; }

OperatorNodePtr PhysicalNonKeyedSlidingWindowSink::copy() {
    return create(inputSchema, outputSchema, keyedEventTimeWindowHandler, windowDefinition);
}

}// namespace NES::QueryCompilation::PhysicalOperators