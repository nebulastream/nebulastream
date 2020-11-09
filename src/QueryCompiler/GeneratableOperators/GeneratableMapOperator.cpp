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

#include <Nodes/Expressions/FieldAssignmentExpressionNode.hpp>
#include <Phases/TranslateToLegacyPlanPhase.hpp>
#include <QueryCompiler/CodeGenerator.hpp>
#include <QueryCompiler/GeneratableOperators/GeneratableMapOperator.hpp>

namespace NES {

void GeneratableMapOperator::produce(CodeGeneratorPtr codegen, PipelineContextPtr context) {
    getChildren()[0]->as<GeneratableOperator>()->produce(codegen, context);
}

void GeneratableMapOperator::consume(CodeGeneratorPtr codegen, PipelineContextPtr context) {
    auto field = getMapExpression()->getField();
    auto assignment = getMapExpression()->getAssignment();
    // todo remove if code gen can handle expressions
    auto mapExpression = TranslateToLegacyPlanPhase::create()->transformExpression(assignment);
    codegen->generateCodeForMap(AttributeField::create(field->getFieldName(), field->getStamp()), mapExpression, context);
    getParents()[0]->as<GeneratableOperator>()->consume(codegen, context);
}

GeneratableMapOperatorPtr GeneratableMapOperator::create(MapLogicalOperatorNodePtr mapLogicalOperator, OperatorId id) {
    return std::make_shared<GeneratableMapOperator>(GeneratableMapOperator(mapLogicalOperator->getMapExpression(), id));
}

GeneratableMapOperator::GeneratableMapOperator(FieldAssignmentExpressionNodePtr mapExpression, OperatorId id) : MapLogicalOperatorNode(mapExpression, id) {
}

const std::string GeneratableMapOperator::toString() const {
    std::stringstream ss;
    ss << "GENERATABLE_MAP(" << outputSchema->toString() << ")";
    return ss.str();
}

}// namespace NES