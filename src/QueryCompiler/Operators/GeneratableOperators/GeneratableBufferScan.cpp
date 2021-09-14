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

#include <QueryCompiler/CodeGenerator/CodeGenerator.hpp>
#include <QueryCompiler/Operators/GeneratableOperators/GeneratableBufferScan.hpp>
#include <Util/UtilityFunctions.hpp>
#include <utility>

namespace NES::QueryCompilation::GeneratableOperators {

GeneratableBufferScan::GeneratableBufferScan(OperatorId id, const SchemaPtr& inputSchema)
    : OperatorNode(id), GeneratableOperator(id, inputSchema, inputSchema) {}

GeneratableOperatorPtr GeneratableBufferScan::create(SchemaPtr inputSchema) {
    return create(Util::getNextOperatorId(), std::move(inputSchema));
}

GeneratableOperatorPtr GeneratableBufferScan::create(OperatorId id, SchemaPtr inputSchema) {
    return std::make_shared<GeneratableBufferScan>(GeneratableBufferScan(id, std::move(inputSchema)));
}

void GeneratableBufferScan::generateOpen(CodeGeneratorPtr codegen, PipelineContextPtr context) {
    codegen->generateCodeForScanSetup(context);
}

void GeneratableBufferScan::generateExecute(CodeGeneratorPtr codegen, PipelineContextPtr context) {
    codegen->generateCodeForScan(inputSchema, inputSchema, context);
}

std::string GeneratableBufferScan::toString() const { return "GeneratableBufferScan"; }

OperatorNodePtr GeneratableBufferScan::copy() { return create(id, inputSchema); }

}// namespace NES::QueryCompilation::GeneratableOperators