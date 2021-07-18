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
#include <Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Plans/Utils/QueryPlanIterator.hpp>
#include <QueryCompiler/Operators/GeneratableOperators/GeneratableBufferEmit.hpp>
#include <QueryCompiler/Operators/GeneratableOperators/GeneratableBufferScan.hpp>
#include <QueryCompiler/Operators/GeneratableOperators/GeneratableFilterOperator.hpp>
#include <QueryCompiler/Operators/OperatorPipeline.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalOperator.hpp>
#include <QueryCompiler/Phases/Translations/GeneratableOperatorProvider.hpp>
#include <QueryCompiler/Phases/BufferOptimizationPhase.hpp>
#include <QueryCompiler/QueryCompilerForwardDeclaration.hpp>
#include <utility>

namespace NES::QueryCompilation {

BufferOptimizationPhasePtr
BufferOptimizationPhase::BufferOptimizationPhase::create(BufferOptimizationStrategy desiredOptimization) {
    return std::make_shared<BufferOptimizationPhase>(desiredOptimization);
}

BufferOptimizationPhase::BufferOptimizationPhase(BufferOptimizationStrategy desiredOptimization)
    : desiredStrategy(desiredOptimization) {}  // TODO move needed for enum ?

PipelineQueryPlanPtr BufferOptimizationPhase::apply(PipelineQueryPlanPtr pipelinedQueryPlan) {
    for (const auto& pipeline : pipelinedQueryPlan->getPipelines()) {
        if (pipeline->isOperatorPipeline()) {
            apply(pipeline);
        }
    }
    return pipelinedQueryPlan;
}

OperatorPipelinePtr BufferOptimizationPhase::apply(OperatorPipelinePtr operatorPipeline) {

    // TODO: set the desired Strategy in the correct place (setDefaultOptions or somewhere)
    desiredStrategy = ONLY_INPLACE_OPERATIONS;

    auto queryPlan = operatorPipeline->getQueryPlan();
    auto nodes = QueryPlanIterator(queryPlan).snapshot();

    SchemaPtr inputSchema = nullptr;
    SchemaPtr outputSchema = nullptr;
    std::shared_ptr<GeneratableOperators::GeneratableBufferEmit> emitNode = nullptr;
    bool filterOperatorFound = false; // TODO other checks required? other operators to look out for?

    for (const auto& node : nodes) {
        if (node->instanceOf<GeneratableOperators::GeneratableBufferScan>()) {
            auto scanNode = node->as<GeneratableOperators::GeneratableBufferScan>();
            inputSchema = scanNode->getInputSchema();
        } else if (node->instanceOf<GeneratableOperators::GeneratableBufferEmit>()) {
            emitNode = node->as<GeneratableOperators::GeneratableBufferEmit>();
            outputSchema = emitNode->getOutputSchema();
        } else if (node->instanceOf<GeneratableOperators::GeneratableFilterOperator>()) {
            filterOperatorFound = true;
        }
    }

    if (inputSchema == nullptr) { NES_ERROR("BufferOptimizationPhase: No Scan operator found in pipeline"); }
    if (emitNode == nullptr || outputSchema == nullptr) { NES_ERROR("BufferOptimizationPhase: No Emit operator found in pipeline"); }

    // Check if necessary conditions are fulfilled and set the desired strategy in the emit operator:
    if (desiredStrategy == ONLY_INPLACE_OPERATIONS && inputSchema->equals(outputSchema) && !filterOperatorFound) {
        // The highest level of optimization - just modifying the input buffer in place and passing it on - can be applied as there are no filter statements etc.
        emitNode->setBufferOptimizationStrategy(ONLY_INPLACE_OPERATIONS);
    } else if (inputSchema->getSchemaSizeInBytes() >= outputSchema->getSchemaSizeInBytes()) {
        // The optimizations  "reuse input buffer as output buffer" and "omit size check" can be applied (or both). The following line might also write pass on "DEFAULT" if it is the desiredStrategy.
        emitNode->setBufferOptimizationStrategy(desiredStrategy);
    }
    // TODO if the highest optimization level ONLY_INPLACE_OPERATIONS is requested, but can't be fulfilled do we want to fall back on a lesser optimization or just use default?

    return operatorPipeline;
}

}// namespace NES::QueryCompilation