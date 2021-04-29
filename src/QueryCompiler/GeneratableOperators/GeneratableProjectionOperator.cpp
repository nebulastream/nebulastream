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

#include <Operators/OperatorForwardDeclaration.hpp>
#include <QueryCompiler/CodeGenerator.hpp>
#include <QueryCompiler/GeneratableOperators/GeneratableProjectionOperator.hpp>
namespace NES {

GeneratableProjectionOperator::GeneratableProjectionOperator(std::vector<ExpressionNodePtr> expressions, OperatorId id)
    : OperatorNode(id), ProjectionLogicalOperatorNode(expressions, id) {}

GeneratableProjectionOperatorPtr GeneratableProjectionOperator::create(ProjectionLogicalOperatorNodePtr projectLogicalOperator,
                                                                       OperatorId id) {
    return std::make_shared<GeneratableProjectionOperator>(
        GeneratableProjectionOperator(projectLogicalOperator->getExpressions(), id));
}

void GeneratableProjectionOperator::produce(CodeGeneratorPtr codegen, PipelineContextPtr context) {
    getChildren()[0]->as<GeneratableOperator>()->produce(codegen, context);
}

void GeneratableProjectionOperator::consume(CodeGeneratorPtr codegen, PipelineContextPtr context) {
    codegen->generateCodeForProjection(getExpressions(), context);
    getParents()[0]->as<GeneratableOperator>()->consume(codegen, context);
}

const std::string GeneratableProjectionOperator::toString() const {
    std::stringstream ss;
    ss << "GENERATABLE_PROJECTION(" << outputSchema->toString() << ")";
    return ss.str();
}

}// namespace NES