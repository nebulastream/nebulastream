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
#include <Operators/LogicalOperators/JoinLogicalOperatorNode.hpp>
#include <QueryCompiler/CodeGenerator.hpp>
#include <QueryCompiler/GeneratableOperators/GeneratableJoinOperator.hpp>
#include <QueryCompiler/PipelineContext.hpp>

namespace NES {

void GeneratableJoinOperator::produce(CodeGeneratorPtr codegen, PipelineContextPtr context) {
    auto newPipelineContext1 = PipelineContext::create();
    newPipelineContext1->isLeftSide = true;
    getChildren()[0]->as<GeneratableOperator>()->produce(codegen, newPipelineContext1);

    auto newPipelineContext2 = PipelineContext::create();
    newPipelineContext2->isLeftSide = false;
    getChildren()[1]->as<GeneratableOperator>()->produce(codegen, newPipelineContext2);

    context->addNextPipeline(newPipelineContext1);
    context->addNextPipeline(newPipelineContext2);
}

void GeneratableJoinOperator::consume(CodeGeneratorPtr codegen, PipelineContextPtr context) {
    auto joinOperatorHandlerIndex = codegen->generateJoinSetup(joinDefinition, context);
    codegen->generateCodeForJoin(joinDefinition, context, joinOperatorHandlerIndex);
}

GeneratableJoinOperatorPtr GeneratableJoinOperator::create(JoinLogicalOperatorNodePtr logicalJoinOperator, OperatorId id) {
    return std::make_shared<GeneratableJoinOperator>(
        GeneratableJoinOperator(logicalJoinOperator->getOutputSchema(), logicalJoinOperator->getJoinDefinition(), id));
}

GeneratableJoinOperator::GeneratableJoinOperator(SchemaPtr schema, Join::LogicalJoinDefinitionPtr joinDefinition, OperatorId id)
    : JoinLogicalOperatorNode(joinDefinition, id), joinDefinition(joinDefinition) {

    if (!schema) {
        NES_ERROR("GeneratableJoinOperator invalid schema");
    }
    setLeftInputSchema(schema);
    setRightInputSchema(schema);
    setOutputSchema(schema);
}

const std::string GeneratableJoinOperator::toString() const {
    std::stringstream ss;
    ss << "JOIN_(" << outputSchema->toString() << ")";
    return ss.str();
}

}// namespace NES