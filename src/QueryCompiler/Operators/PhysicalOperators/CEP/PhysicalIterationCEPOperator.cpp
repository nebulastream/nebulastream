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
#include <QueryCompiler/Operators/PhysicalOperators/CEP/PhysicalIterationCEPOperator.hpp>

namespace NES {
namespace QueryCompilation {
namespace PhysicalOperators {

PhysicalIterationCEPOperator::PhysicalIterationCEPOperator(OperatorId id, SchemaPtr inputSchema, SchemaPtr outputSchema,
                                               uint64_t minIterations, uint64_t maxIterations)
    : OperatorNode(id), PhysicalUnaryOperator(id, inputSchema, outputSchema), minIterations(minIterations), maxIterations(maxIterations)  {}

PhysicalOperatorPtr PhysicalIterationCEPOperator::create(OperatorId id, SchemaPtr inputSchema, SchemaPtr outputSchema, uint64_t minIterations, uint64_t maxIterations) {
    return std::make_shared<PhysicalIterationCEPOperator>(id, inputSchema, outputSchema, minIterations, maxIterations);
}

uint64_t PhysicalIterationCEPOperator::getMaxIterations() { return maxIterations; }

uint64_t PhysicalIterationCEPOperator::getMinIterations() { return minIterations; }

PhysicalOperatorPtr PhysicalIterationCEPOperator::create(SchemaPtr inputSchema, SchemaPtr outputSchema, uint64_t minIterations, uint64_t maxIterations) {
    return create(UtilityFunctions::getNextOperatorId(), inputSchema, outputSchema, minIterations, maxIterations);
}

const std::string PhysicalIterationCEPOperator::toString() const { return "PhysicalIterationCEPOperator"; }

OperatorNodePtr PhysicalIterationCEPOperator::copy() { return create(id, inputSchema, outputSchema, getMinIterations(), getMaxIterations()); }

}// namespace PhysicalOperators
}// namespace QueryCompilation
}// namespace NES