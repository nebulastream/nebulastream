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
#include <QueryCompiler/GeneratableOperators/GeneratableMergeOperator.hpp>
#include <QueryCompiler/PipelineContext.hpp>

namespace NES {

void GeneratableMergeOperator::produce(CodeGeneratorPtr codegen, PipelineContextPtr context) {
    auto newPipelineContext1 = PipelineContext::create();
    getChildren()[0]->as<GeneratableOperator>()->produce(codegen, newPipelineContext1);
    auto newPipelineContext2 = PipelineContext::create();
    getChildren()[1]->as<GeneratableOperator>()->produce(codegen, newPipelineContext2);

    context->addNextPipeline(newPipelineContext1);
    context->addNextPipeline(newPipelineContext2);
}

void GeneratableMergeOperator::consume(CodeGeneratorPtr codegen, PipelineContextPtr context) {
    codegen->generateCodeForEmit(outputSchema, context);
}

GeneratableMergeOperatorPtr GeneratableMergeOperator::create(MergeLogicalOperatorNodePtr logicalMergeOperator, OperatorId id) {
    return std::make_shared<GeneratableMergeOperator>(GeneratableMergeOperator(logicalMergeOperator->getOutputSchema(), id));
}

GeneratableMergeOperator::GeneratableMergeOperator(SchemaPtr schema, OperatorId id) : MergeLogicalOperatorNode(id) {
    setInputSchema(schema);
    setOutputSchema(schema);
}

const std::string GeneratableMergeOperator::toString() const {
    std::stringstream ss;
    ss << "GENERATABLE_MERGE(" << outputSchema->toString() << ")";
    return ss.str();
}

}// namespace NES