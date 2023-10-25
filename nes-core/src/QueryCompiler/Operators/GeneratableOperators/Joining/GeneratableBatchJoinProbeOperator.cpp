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
#include <QueryCompiler/Operators/GeneratableOperators/Joining/GeneratableBatchJoinProbeOperator.hpp>
#include <Util/Core.hpp>
#include <Windowing/WindowHandler/BatchJoinOperatorHandler.hpp>
#include <utility>

namespace NES::QueryCompilation::GeneratableOperators {

GeneratableBatchJoinProbeOperator::GeneratableBatchJoinProbeOperator(
    OperatorId id,
    SchemaPtr inputSchema,
    SchemaPtr outputSchema,
    Join::Experimental::BatchJoinOperatorHandlerPtr batchJoinOperatorHandler)
    : OperatorNode(id),
      GeneratableBatchJoinOperator(id, std::move(inputSchema), std::move(outputSchema), std::move(batchJoinOperatorHandler)) {}

GeneratableOperatorPtr
GeneratableBatchJoinProbeOperator::create(OperatorId id,
                                          SchemaPtr inputSchema,
                                          SchemaPtr outputSchema,
                                          Join::Experimental::BatchJoinOperatorHandlerPtr batchJoinOperatorHandler) {
    return std::make_shared<GeneratableBatchJoinProbeOperator>(
        GeneratableBatchJoinProbeOperator(id,
                                          std::move(inputSchema),
                                          std::move(outputSchema),
                                          std::move(batchJoinOperatorHandler)));
}

GeneratableOperatorPtr
GeneratableBatchJoinProbeOperator::create(SchemaPtr inputSchema,
                                          SchemaPtr outputSchema,
                                          Join::Experimental::BatchJoinOperatorHandlerPtr batchJoinOperatorHandler) {
    return create(getNextOperatorId(), std::move(inputSchema), std::move(outputSchema), std::move(batchJoinOperatorHandler));
}

void GeneratableBatchJoinProbeOperator::generateExecute(CodeGeneratorPtr codegen, PipelineContextPtr context) {
    auto batchJoinDefinition = batchJoinOperatorHandler->getBatchJoinDefinition();
    codegen->generateCodeForBatchJoinProbe(batchJoinDefinition, context, batchJoinOperatorHandler);
}
std::string GeneratableBatchJoinProbeOperator::toString() const { return "GeneratableBatchJoinProbeOperator"; }

OperatorNodePtr GeneratableBatchJoinProbeOperator::copy() {
    return create(id, inputSchema, outputSchema, batchJoinOperatorHandler);
}

}// namespace NES::QueryCompilation::GeneratableOperators