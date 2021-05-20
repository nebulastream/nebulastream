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
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalInferModelOperator.hpp>

namespace NES {
namespace QueryCompilation {
namespace PhysicalOperators {

PhysicalInferModelOperator::PhysicalInferModelOperator(OperatorId id,
                                         SchemaPtr inputSchema,
                                         SchemaPtr outputSchema,
                                         std::string model)
    : OperatorNode(id), PhysicalUnaryOperator(id, inputSchema, outputSchema), model(model) {}

PhysicalOperatorPtr PhysicalInferModelOperator::create(OperatorId id,
                                                SchemaPtr inputSchema,
                                                SchemaPtr outputSchema,
                                                std::string model) {
    return std::make_shared<PhysicalInferModelOperator>(id, inputSchema, outputSchema, model);
}

PhysicalOperatorPtr
PhysicalInferModelOperator::create(SchemaPtr inputSchema, SchemaPtr outputSchema, std::string model,
                                   std::vector<ExpressionItem> inputFields,
                                   std::vector<ExpressionItem> outputFields) {
    return create(UtilityFunctions::getNextOperatorId(), inputSchema, outputSchema, model);
}

const std::string PhysicalInferModelOperator::toString() const { return "PhysicalInferModelOperator"; }

OperatorNodePtr PhysicalInferModelOperator::copy() { return create(id, inputSchema, outputSchema, model); }

}// namespace PhysicalOperators
}// namespace QueryCompilation
}// namespace NES