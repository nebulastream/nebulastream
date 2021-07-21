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
    if (desiredStrategy == NO_OPTIMIZATION) {
        NES_DEBUG("BufferOptimizationPhase: No optimization requested or applied.");
        return operatorPipeline;
    }

    NES_DEBUG("BufferOptimizationPhase: Scanning pipeline for optimization potential.");
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

    if (inputSchema == nullptr) { NES_FATAL_ERROR("BufferOptimizationPhase: No Scan operator found in pipeline"); }
    if (emitNode == nullptr || outputSchema == nullptr) { NES_FATAL_ERROR("BufferOptimizationPhase: No Emit operator found in pipeline"); }

    // Check if necessary conditions are fulfilled and set the desired strategy in the emit operator:
    if (inputSchema->equals(outputSchema) && !filterOperatorFound && (desiredStrategy == ONLY_INPLACE_OPERATIONS || desiredStrategy == HIGHEST_POSSIBLE)) {
        // The highest level of optimization - just modifying the input buffer in place and passing it on - can be applied as there are no filter statements etc.
        emitNode->setBufferOptimizationStrategy(ONLY_INPLACE_OPERATIONS);
        NES_DEBUG("BufferOptimizationPhase: Assign ONLY_INPLACE_OPERATIONS optimization strategy to pipeline.");
    } else if (inputSchema->getSchemaSizeInBytes() >= outputSchema->getSchemaSizeInBytes() && (desiredStrategy == REUSE_INPUT_BUFFER_AND_OMIT_OVERFLOW_CHECK || desiredStrategy == HIGHEST_POSSIBLE)) {
        // The optimizations  "reuse input buffer as output buffer" and "omit size check" can be applied.
        emitNode->setBufferOptimizationStrategy(REUSE_INPUT_BUFFER_AND_OMIT_OVERFLOW_CHECK);
        NES_DEBUG("BufferOptimizationPhase: Assign REUSE_INPUT_BUFFER_AND_OMIT_OVERFLOW_CHECK optimization strategy to pipeline.");
    } else if (inputSchema->getSchemaSizeInBytes() >= outputSchema->getSchemaSizeInBytes() && (desiredStrategy == REUSE_INPUT_BUFFER || desiredStrategy == HIGHEST_POSSIBLE)) {
        // The optimization  "reuse input buffer as output buffer" can be applied.
        emitNode->setBufferOptimizationStrategy(REUSE_INPUT_BUFFER);
        NES_DEBUG("BufferOptimizationPhase: Assign REUSE_INPUT_BUFFER optimization strategy to pipeline.");
    } else if (inputSchema->getSchemaSizeInBytes() >= outputSchema->getSchemaSizeInBytes() && (desiredStrategy == OMIT_OVERFLOW_CHECK || desiredStrategy == HIGHEST_POSSIBLE)) {
        // The optimization "omit size check" can be applied.
        emitNode->setBufferOptimizationStrategy(OMIT_OVERFLOW_CHECK);
        NES_DEBUG("BufferOptimizationPhase: Assign OMIT_OVERFLOW_CHECK optimization strategy to pipeline.");
    }

    return operatorPipeline;
}

}// namespace NES::QueryCompilation