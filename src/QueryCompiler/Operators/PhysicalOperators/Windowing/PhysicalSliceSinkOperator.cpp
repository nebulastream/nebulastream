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
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/PhysicalSliceSinkOperator.hpp>

namespace NES {
namespace QueryCompilation {
namespace PhysicalOperators {

PhysicalOperatorPtr PhysicalSliceSinkOperator::create(SchemaPtr inputSchema, SchemaPtr outputSchema, Windowing::LogicalWindowDefinitionPtr windowDefinition) {
    return create(UtilityFunctions::getNextOperatorId(), inputSchema, outputSchema, windowDefinition);
}

PhysicalOperatorPtr PhysicalSliceSinkOperator::create(OperatorId id, SchemaPtr inputSchema, SchemaPtr outputSchema, Windowing::LogicalWindowDefinitionPtr windowDefinition) {
    return std::make_shared<PhysicalSliceSinkOperator>(id, inputSchema, outputSchema, windowDefinition);
}

PhysicalSliceSinkOperator::PhysicalSliceSinkOperator(OperatorId id, SchemaPtr inputSchema, SchemaPtr outputSchema, Windowing::LogicalWindowDefinitionPtr windowDefinition)
    : OperatorNode(id), PhysicalWindowOperator(id, inputSchema, outputSchema, windowDefinition) {};

const std::string PhysicalSliceSinkOperator::toString() const {
    return "PhysicalSliceSinkOperator";
}

OperatorNodePtr PhysicalSliceSinkOperator::copy() {
    return create(id, inputSchema, outputSchema, windowDefinition);
}

}// namespace PhysicalOperators
}// namespace QueryCompilation
}// namespace NES