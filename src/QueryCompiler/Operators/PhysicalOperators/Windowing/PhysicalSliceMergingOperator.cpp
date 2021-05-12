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
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/PhysicalSliceMergingOperator.hpp>

namespace NES {
namespace QueryCompilation {
namespace PhysicalOperators {

PhysicalOperatorPtr
PhysicalSliceMergingOperator::create(SchemaPtr inputSchema, SchemaPtr outputSchema, Windowing::WindowOperatorHandlerPtr handler) {
    return create(UtilityFunctions::getNextOperatorId(), inputSchema, outputSchema, handler);
}

PhysicalOperatorPtr PhysicalSliceMergingOperator::create(OperatorId id,
                                                         SchemaPtr inputSchema,
                                                         SchemaPtr outputSchema,
                                                         Windowing::WindowOperatorHandlerPtr handler) {
    return std::make_shared<PhysicalSliceMergingOperator>(id, inputSchema, outputSchema, handler);
}

PhysicalSliceMergingOperator::PhysicalSliceMergingOperator(OperatorId id,
                                                           SchemaPtr inputSchema,
                                                           SchemaPtr outputSchema,
                                                           Windowing::WindowOperatorHandlerPtr handler)
    : OperatorNode(id), PhysicalWindowOperator(id, inputSchema, outputSchema, handler){};

const std::string PhysicalSliceMergingOperator::toString() const { return "PhysicalSliceMergingOperator"; }

OperatorNodePtr PhysicalSliceMergingOperator::copy() { return create(id, inputSchema, outputSchema, operatorHandler); }

}// namespace PhysicalOperators
}// namespace QueryCompilation
}// namespace NES