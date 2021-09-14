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
#include <QueryCompiler/Operators/GeneratableOperators/Joining/GeneratableJoinSinkOperator.hpp>
#include <Util/UtilityFunctions.hpp>
#include <Windowing/WindowHandler/JoinOperatorHandler.hpp>
#include <utility>

namespace NES::QueryCompilation::GeneratableOperators {

GeneratableJoinSinkOperator::GeneratableJoinSinkOperator(OperatorId id,
                                                         SchemaPtr inputSchema,
                                                         SchemaPtr outputSchema,
                                                         Join::JoinOperatorHandlerPtr operatorHandler)
    : OperatorNode(id), GeneratableJoinOperator(id, std::move(inputSchema), std::move(outputSchema), std::move(operatorHandler)) {
}

GeneratableOperatorPtr GeneratableJoinSinkOperator::create(OperatorId id,
                                                           SchemaPtr inputSchema,
                                                           SchemaPtr outputSchema,
                                                           Join::JoinOperatorHandlerPtr operatorHandler) {
    return std::make_shared<GeneratableJoinSinkOperator>(
        GeneratableJoinSinkOperator(id, std::move(inputSchema), std::move(outputSchema), std::move(operatorHandler)));
}

GeneratableOperatorPtr
GeneratableJoinSinkOperator::create(SchemaPtr inputSchema, SchemaPtr outputSchema, Join::JoinOperatorHandlerPtr operatorHandler) {
    return create(Util::getNextOperatorId(),
                  std::move(inputSchema),
                  std::move(outputSchema),
                  std::move(operatorHandler));
}

void GeneratableJoinSinkOperator::generateOpen(CodeGeneratorPtr codegen, PipelineContextPtr context) {
    //auto joinDefinition = operatorHandler->getJoinDefinition();
    //codegen->generateCodeForJoinSinkSetup(joinDefinition, context, this->id, operatorHandler);
    codegen->generateCodeForScanSetup(context);
}

void GeneratableJoinSinkOperator::generateExecute(CodeGeneratorPtr codegen, PipelineContextPtr context) {
    codegen->generateCodeForScan(outputSchema, outputSchema, context);
}
std::string GeneratableJoinSinkOperator::toString() const { return "GeneratableJoinSinkOperator"; }

OperatorNodePtr GeneratableJoinSinkOperator::copy() { return create(id, inputSchema, outputSchema, operatorHandler); }

}// namespace NES::QueryCompilation::GeneratableOperators