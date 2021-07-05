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

#include <Nodes/Expressions/ExpressionNode.hpp>
#include <Nodes/Expressions/FieldAssignmentExpressionNode.hpp>
#include <QueryCompiler/CodeGenerator/CodeGenerator.hpp>
#include <QueryCompiler/CodeGenerator/TranslateToLegacyExpression.hpp>
#include <QueryCompiler/Operators/GeneratableOperators/CEP/GeneratableCEPIterationOperator.hpp>
#include <Util/UtilityFunctions.hpp>

namespace NES {
namespace QueryCompilation {
namespace GeneratableOperators {

GeneratableCEPIterationOperator::GeneratableCEPIterationOperator(OperatorId id,
                                               SchemaPtr inputSchema,
                                               SchemaPtr outputSchema,
                                               uint64_t minIteration,
                                               uint64_t maxIteration)
    : OperatorNode(id), GeneratableOperator(id, inputSchema, outputSchema), minIteration(minIteration), maxIteration(maxIteration) {}

GeneratableOperatorPtr GeneratableCEPIterationOperator::create(SchemaPtr inputSchema, SchemaPtr outputSchema, uint64_t minIteration, uint64_t maxIteration) {
    return create(UtilityFunctions::getNextOperatorId(), inputSchema, outputSchema, minIteration, maxIteration);
}

GeneratableOperatorPtr GeneratableCEPIterationOperator::create(OperatorId id,
                                                      SchemaPtr inputSchema,
                                                      SchemaPtr outputSchema,
                                                      uint64_t minIteration,
                                                      uint64_t maxIteration) {
    return std::make_shared<GeneratableCEPIterationOperator>(GeneratableCEPIterationOperator(id, inputSchema, outputSchema, minIteration, maxIteration));
}

void GeneratableCEPIterationOperator::generateExecute(CodeGeneratorPtr codegen, PipelineContextPtr context) {

    codegen->generateCodeForCEPIteration(minIteration, maxIteration, context);
}

OperatorNodePtr GeneratableCEPIterationOperator::copy() { return create(id, inputSchema, outputSchema, minIteration, maxIteration); }
std::string GeneratableCEPIterationOperator::toString() const { return "GeneratableCEPIterationOperator"; }

}// namespace GeneratableOperators
}// namespace QueryCompilation
}// namespace NES