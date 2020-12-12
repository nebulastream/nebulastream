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
#include <QueryCompiler/GeneratableOperators/GeneratableScanOperator.hpp>
#include <QueryCompiler/PipelineContext.hpp>

namespace NES {

void GeneratableScanOperator::produce(CodeGeneratorPtr codegen, PipelineContextPtr context) {
    this->consume(codegen, context);
    if (!getChildren().empty()) {
        getChildren()[0]->as<GeneratableOperator>()->produce(codegen, context);
    }
}

void GeneratableScanOperator::consume(CodeGeneratorPtr codegen, PipelineContextPtr context) {
    codegen->generateCodeForScan(inputSchema, outputSchema, context);
    getParents()[0]->as<GeneratableOperator>()->consume(codegen, context);
}

OperatorNodePtr GeneratableScanOperator::copy() { return GeneratableScanOperator::create(inputSchema, outputSchema, id); }

GeneratableScanOperatorPtr GeneratableScanOperator::create(SchemaPtr inputSchema, SchemaPtr outputSchema, OperatorId id) {
    return std::make_shared<GeneratableScanOperator>(GeneratableScanOperator(inputSchema, outputSchema, id));
}

GeneratableScanOperator::GeneratableScanOperator(SchemaPtr inputSchema, SchemaPtr outputSchema,  OperatorId id) :
                                                                                                                 inputSchema(inputSchema->copy()),
                                                                                                                 outputSchema(outputSchema->copy()),
                                                                                                                 OperatorNode(id) {}

const std::string GeneratableScanOperator::toString() const {
    std::stringstream ss;
    ss << "GENERATABLE_SCAN(" << outputSchema->toString() << ")";
    return ss.str();
}

}// namespace NES