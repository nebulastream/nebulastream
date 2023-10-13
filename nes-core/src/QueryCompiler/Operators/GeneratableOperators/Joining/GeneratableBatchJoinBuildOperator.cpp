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
#include <QueryCompiler/Operators/GeneratableOperators/Joining/GeneratableBatchJoinBuildOperator.hpp>
#include <Util/Core.hpp>
#include <Windowing/WindowHandler/BatchJoinOperatorHandler.hpp>
#include <utility>

namespace NES::QueryCompilation::GeneratableOperators {

GeneratableOperatorPtr
GeneratableBatchJoinBuildOperator::create(OperatorId id,
                                          SchemaPtr inputSchema,
                                          SchemaPtr outputSchema,
                                          Join::Experimental::BatchJoinOperatorHandlerPtr batchJoinOperatorHandler) {
    return std::make_shared<GeneratableBatchJoinBuildOperator>(
        GeneratableBatchJoinBuildOperator(id,
                                          std::move(inputSchema),
                                          std::move(outputSchema),
                                          std::move(batchJoinOperatorHandler)));
}

GeneratableOperatorPtr
GeneratableBatchJoinBuildOperator::create(SchemaPtr inputSchema,
                                          SchemaPtr outputSchema,
                                          Join::Experimental::BatchJoinOperatorHandlerPtr batchJoinOperatorHandler) {
    return create(getNextOperatorId(),
                  std::move(inputSchema),
                  std::move(outputSchema),
                  std::move(batchJoinOperatorHandler));
}

GeneratableBatchJoinBuildOperator::GeneratableBatchJoinBuildOperator(
    OperatorId id,
    SchemaPtr inputSchema,
    SchemaPtr outputSchema,
    Join::Experimental::BatchJoinOperatorHandlerPtr batchJoinOperatorHandler)
    : OperatorNode(id),
      GeneratableBatchJoinOperator(id, std::move(inputSchema), std::move(outputSchema), std::move(batchJoinOperatorHandler)) {}

void GeneratableBatchJoinBuildOperator::generateOpen(CodeGeneratorPtr codegen, PipelineContextPtr context) {
    auto batchJoinDefinition = batchJoinOperatorHandler->getBatchJoinDefinition();
    codegen->generateCodeForBatchJoinHandlerSetup(batchJoinDefinition, context, this->id, batchJoinOperatorHandler);
}

void GeneratableBatchJoinBuildOperator::generateExecute(CodeGeneratorPtr codegen, PipelineContextPtr context) {
    auto batchJoinDefinition = batchJoinOperatorHandler->getBatchJoinDefinition();
    codegen->generateCodeForBatchJoinBuild(batchJoinDefinition, context, batchJoinOperatorHandler);
}

std::string GeneratableBatchJoinBuildOperator::toString() const { return "GeneratableBatchJoinBuildOperator"; }

OperatorNodePtr GeneratableBatchJoinBuildOperator::copy() {
    return create(id, inputSchema, outputSchema, batchJoinOperatorHandler);
}

}// namespace NES::QueryCompilation::GeneratableOperators