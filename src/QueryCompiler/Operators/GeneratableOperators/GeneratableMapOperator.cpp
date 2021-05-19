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
#include <Phases/TranslateToLegacyPlanPhase.hpp>
#include <QueryCompiler/CodeGenerator/CodeGenerator.hpp>
#include <QueryCompiler/Operators/GeneratableOperators/GeneratableMapOperator.hpp>
#include <Util/UtilityFunctions.hpp>

namespace NES {
namespace QueryCompilation {
namespace GeneratableOperators {

GeneratableMapOperator::GeneratableMapOperator(OperatorId id,
                                               SchemaPtr inputSchema,
                                               SchemaPtr outputSchema,
                                               FieldAssignmentExpressionNodePtr mapExpression)
    : OperatorNode(id), GeneratableOperator(id, inputSchema, outputSchema), mapExpression(mapExpression) {}

GeneratableOperatorPtr
GeneratableMapOperator::create(SchemaPtr inputSchema, SchemaPtr outputSchema, FieldAssignmentExpressionNodePtr mapExpression) {
    return create(UtilityFunctions::getNextOperatorId(), inputSchema, outputSchema, mapExpression);
}

GeneratableOperatorPtr GeneratableMapOperator::create(OperatorId id,
                                                      SchemaPtr inputSchema,
                                                      SchemaPtr outputSchema,
                                                      FieldAssignmentExpressionNodePtr mapExpression) {
    return std::make_shared<GeneratableMapOperator>(GeneratableMapOperator(id, inputSchema, outputSchema, mapExpression));
}

void GeneratableMapOperator::generateExecute(CodeGeneratorPtr codegen, PipelineContextPtr context) {
    auto field = mapExpression->getField();
    auto assignment = mapExpression->getAssignment();
    // todo remove if code gen can handle expressions
    auto legacyMapExpression = TranslateToLegacyPlanPhase::create()->transformExpression(assignment);
    codegen->generateCodeForMap(AttributeField::create(field->getFieldName(), field->getStamp()), legacyMapExpression, context);
}

const std::string GeneratableMapOperator::toString() const { return "GeneratableMapOperator"; }

OperatorNodePtr GeneratableMapOperator::copy() { return create(id, inputSchema, outputSchema, mapExpression); }

}// namespace GeneratableOperators

}// namespace QueryCompilation
}// namespace NES