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

#include <QueryCompiler/Operators/PhysicalOperators/Windowing/KeyedTimeWindow/PhysicalKeyedSlidingWindowSink.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/PhysicalWindowOperator.hpp>
#include <memory>

namespace NES {
namespace QueryCompilation {
namespace PhysicalOperators {

PhysicalKeyedSlidingWindowSink::PhysicalKeyedSlidingWindowSink(
    OperatorId id,
    SchemaPtr inputSchema,
    SchemaPtr outputSchema,
    Windowing::LogicalWindowDefinitionPtr windowDefinition)
    : OperatorNode(id), PhysicalUnaryOperator(id, inputSchema, outputSchema), AbstractScanOperator(), windowDefinition(windowDefinition) {}

std::shared_ptr<PhysicalKeyedSlidingWindowSink> PhysicalKeyedSlidingWindowSink::create(
    SchemaPtr inputSchema,
    SchemaPtr outputSchema,
    Windowing::LogicalWindowDefinitionPtr windowDefinition) {
    return std::make_shared<PhysicalKeyedSlidingWindowSink>(getNextOperatorId(),
                                                            inputSchema,
                                                            outputSchema,

                                                            windowDefinition);
}

std::string PhysicalKeyedSlidingWindowSink::toString() const { return "PhysicalKeyedSlidingWindowSink"; }
Windowing::LogicalWindowDefinitionPtr PhysicalKeyedSlidingWindowSink::getWindowDefinition() { return windowDefinition; }
OperatorNodePtr PhysicalKeyedSlidingWindowSink::copy() {
    return create(inputSchema, outputSchema,  windowDefinition);
}

}// namespace PhysicalOperators
}// namespace QueryCompilation
}// namespace NES