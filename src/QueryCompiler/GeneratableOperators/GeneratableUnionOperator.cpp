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

#include <QueryCompiler/CodeGenerator.hpp>
#include <QueryCompiler/GeneratableOperators/GeneratableUnionOperator.hpp>
#include <Operators/AbstractOperators/Arity/BinaryOperatorNode.hpp>
#include <Operators/LogicalOperators/LogicalBinaryOperatorNode.hpp>
#include <Operators/LogicalOperators/UnionLogicalOperatorNode.hpp>
#include <QueryCompiler/PipelineContext.hpp>

namespace NES {

void GeneratableUnionOperator::produce(CodeGeneratorPtr codegen, PipelineContextPtr context) {
    auto newPipelineContext1 = PipelineContext::create();
    auto newPipelineContext2 = PipelineContext::create();

    newPipelineContext1->arity = PipelineContext::BinaryLeft;
    getChildren()[0]->as<GeneratableOperator>()->produce(codegen, newPipelineContext1);

    newPipelineContext2->arity = PipelineContext::BinaryRight;
    getChildren()[1]->as<GeneratableOperator>()->produce(codegen, newPipelineContext2);

    context->addNextPipeline(newPipelineContext1);
    context->addNextPipeline(newPipelineContext2);
}

void GeneratableUnionOperator::consume(CodeGeneratorPtr codegen, PipelineContextPtr context) {
    codegen->generateCodeForEmit(outputSchema, context);
}

GeneratableMergeOperatorPtr GeneratableUnionOperator::create(UnionLogicalOperatorNodePtr logicalUnionOperator, OperatorId id) {
    return std::make_shared<GeneratableUnionOperator>(GeneratableUnionOperator(logicalUnionOperator->getOutputSchema(), id));
}

GeneratableUnionOperator::GeneratableUnionOperator(SchemaPtr schema, OperatorId id) :  OperatorNode(id), UnionLogicalOperatorNode(id) {
    if (!schema) {
        NES_ERROR("GeneratableMergeOperator invalid schema");
    }
    //FIXME: As part of #1467
    setLeftInputSchema(schema);
    setRightInputSchema(schema);
    setOutputSchema(schema);
}

const std::string GeneratableUnionOperator::toString() const {
    std::stringstream ss;
    ss << "GENERATABLE_MERGE(" << outputSchema->toString() << ")";
    return ss.str();
}

}// namespace NES