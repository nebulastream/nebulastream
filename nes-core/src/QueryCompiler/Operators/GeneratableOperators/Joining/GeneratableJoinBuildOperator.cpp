/*
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
#include <QueryCompiler/Operators/GeneratableOperators/Joining/GeneratableJoinBuildOperator.hpp>
#include <Util/UtilityFunctions.hpp>
#include <Windowing/WindowHandler/JoinOperatorHandler.hpp>
#include <utility>

namespace NES::QueryCompilation::GeneratableOperators {

GeneratableOperatorPtr GeneratableJoinBuildOperator::create(OperatorId id,
                                                            SchemaPtr inputSchema,
                                                            SchemaPtr outputSchema,
                                                            Join::JoinOperatorHandlerPtr operatorHandler,
                                                            JoinBuildSide buildSide) {
    return std::make_shared<GeneratableJoinBuildOperator>(
        GeneratableJoinBuildOperator(id, std::move(inputSchema), std::move(outputSchema), std::move(operatorHandler), buildSide));
}

GeneratableOperatorPtr GeneratableJoinBuildOperator::create(SchemaPtr inputSchema,
                                                            SchemaPtr outputSchema,
                                                            Join::JoinOperatorHandlerPtr operatorHandler,
                                                            JoinBuildSide buildSide) {
    return create(Util::getNextOperatorId(),
                  std::move(inputSchema),
                  std::move(outputSchema),
                  std::move(operatorHandler),
                  buildSide);
}

GeneratableJoinBuildOperator::GeneratableJoinBuildOperator(OperatorId id,
                                                           SchemaPtr inputSchema,
                                                           SchemaPtr outputSchema,
                                                           Join::JoinOperatorHandlerPtr operatorHandler,
                                                           JoinBuildSide buildSide)
    : OperatorNode(id), GeneratableJoinOperator(id, std::move(inputSchema), std::move(outputSchema), std::move(operatorHandler)),
      buildSide(buildSide) {}

void GeneratableJoinBuildOperator::generateOpen(CodeGeneratorPtr codegen, PipelineContextPtr context) {
    if (this->buildSide == Left) {

        auto joinDefinition = operatorHandler->getJoinDefinition();
        codegen->generateCodeForJoinSinkSetup(joinDefinition, context, this->id, operatorHandler);
    }
}

void GeneratableJoinBuildOperator::generateExecute(CodeGeneratorPtr codegen, PipelineContextPtr context) {
    auto joinDefinition = operatorHandler->getJoinDefinition();
    codegen->generateCodeForJoinBuild(joinDefinition, context, operatorHandler, buildSide);
}

std::string GeneratableJoinBuildOperator::toString() const { return "GeneratableJoinBuildOperator"; }

OperatorNodePtr GeneratableJoinBuildOperator::copy() { return create(id, inputSchema, outputSchema, operatorHandler, buildSide); }

}// namespace NES::QueryCompilation::GeneratableOperators