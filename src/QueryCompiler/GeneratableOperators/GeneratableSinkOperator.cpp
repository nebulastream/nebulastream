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
#include <QueryCompiler/GeneratableOperators/GeneratableSinkOperator.hpp>
#include <QueryCompiler/PipelineContext.hpp>
#include <utility>

namespace NES {

void GeneratableSinkOperator::produce(CodeGeneratorPtr codegen, PipelineContextPtr context) {
    getChildren()[0]->as<GeneratableOperator>()->produce(codegen, context);
}

void GeneratableSinkOperator::consume(CodeGeneratorPtr codegen, PipelineContextPtr context) {
    codegen->generateCodeForEmit(outputSchema, context);
}

GeneratableSinkOperatorPtr GeneratableSinkOperator::create(SinkLogicalOperatorNodePtr sinkLogicalOperator, SchemaPtr outputSchema, OperatorId id) {
    return std::make_shared<GeneratableSinkOperator>(GeneratableSinkOperator(sinkLogicalOperator->getSinkDescriptor(), outputSchema, id));
}

GeneratableSinkOperator::GeneratableSinkOperator(SinkDescriptorPtr sinkDescriptor, SchemaPtr outputSchema, OperatorId id)
    : SinkLogicalOperatorNode(std::move(sinkDescriptor), id) {
    this->outputSchema = outputSchema;
}

const std::string GeneratableSinkOperator::toString() const {
    std::stringstream ss;
    ss << "GENERATABLE_SINK(" << outputSchema->toString() << ")";
    return ss.str();
}

}// namespace NES