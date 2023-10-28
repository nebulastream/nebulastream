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

#include <QueryCompiler/Operators/PhysicalOperators/Windowing/NonKeyedTimeWindow/PhysicalNonKeyedTumblingWindowSink.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/PhysicalWindowOperator.hpp>
#include <Operators/LogicalOperators/Windows/LogicalWindowDefinition.hpp>
#include <memory>

namespace NES::QueryCompilation::PhysicalOperators {

PhysicalNonKeyedTumblingWindowSink::PhysicalNonKeyedTumblingWindowSink(OperatorId id,
                                                                       SchemaPtr inputSchema,
                                                                       SchemaPtr outputSchema,
                                                                       Windowing::LogicalWindowDefinitionPtr windowDefinition)
    : OperatorNode(id), PhysicalUnaryOperator(id, inputSchema, outputSchema), windowDefinition(windowDefinition) {}

std::shared_ptr<PhysicalNonKeyedTumblingWindowSink>
PhysicalNonKeyedTumblingWindowSink::create(SchemaPtr inputSchema,
                                           SchemaPtr outputSchema,
                                           Windowing::LogicalWindowDefinitionPtr windowDefinition) {
    return std::make_shared<PhysicalNonKeyedTumblingWindowSink>(getNextOperatorId(), inputSchema, outputSchema, windowDefinition);
}

Windowing::LogicalWindowDefinitionPtr PhysicalNonKeyedTumblingWindowSink::getWindowDefinition() { return windowDefinition; }

std::string PhysicalNonKeyedTumblingWindowSink::toString() const {
    std::stringstream out;
    out << std::endl;
    out << "PhysicalNonKeyedTumblingWindowSink:\n";
    out << PhysicalUnaryOperator::toString();
    out << windowDefinition->toString();
    return out.str();
}

OperatorNodePtr PhysicalNonKeyedTumblingWindowSink::copy() { return create(inputSchema, outputSchema, windowDefinition); }

}// namespace NES::QueryCompilation::PhysicalOperators