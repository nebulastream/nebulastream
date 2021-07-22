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
BufferOptimizationPhase::BufferOptimizationPhase::create(OutputBufferOptimizationLevel level) {
    return std::make_shared<BufferOptimizationPhase>(level);
}

BufferOptimizationPhase::BufferOptimizationPhase(OutputBufferOptimizationLevel level)
    : level(level) {}  // TODO move needed for enum ?

PipelineQueryPlanPtr BufferOptimizationPhase::apply(PipelineQueryPlanPtr pipelinedQueryPlan) {
    for (const auto& pipeline : pipelinedQueryPlan->getPipelines()) {
        if (pipeline->isOperatorPipeline()) {
            apply(pipeline);
        }
    }
    return pipelinedQueryPlan;
}

OperatorPipelinePtr BufferOptimizationPhase::apply(OperatorPipelinePtr operatorPipeline) {
    if (level == NO) {
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
    if (inputSchema->equals(outputSchema) && !filterOperatorFound && (level == ONLY_INPLACE_OPERATIONS_NO_FALLBACK || level == ALL)) {
        // The highest level of optimization - just modifying the input buffer in place and passing it on - can be applied as there are no filter statements etc.
        emitNode->setOutputBufferAllocationStrategy(ONLY_INPLACE_OPERATIONS);
        NES_DEBUG("BufferOptimizationPhase: Assign ONLY_INPLACE_OPERATIONS optimization strategy to pipeline.");
        return operatorPipeline;
    }
    if (inputSchema->getSchemaSizeInBytes() >= outputSchema->getSchemaSizeInBytes() && (level == REUSE_INPUT_BUFFER_AND_OMIT_OVERFLOW_CHECK_NO_FALLBACK || level == ALL)) {
        // The optimizations  "reuse input buffer as output buffer" and "omit size check" can be applied.
        emitNode->setOutputBufferAllocationStrategy(REUSE_INPUT_BUFFER_AND_OMIT_OVERFLOW_CHECK);
        NES_DEBUG("BufferOptimizationPhase: Assign REUSE_INPUT_BUFFER_AND_OMIT_OVERFLOW_CHECK optimization strategy to pipeline.");
        return operatorPipeline;
    }
    if (inputSchema->getSchemaSizeInBytes() >= outputSchema->getSchemaSizeInBytes() && (level == REUSE_INPUT_BUFFER_NO_FALLBACK || level == ALL)) {
        // The optimization  "reuse input buffer as output buffer" can be applied.
        emitNode->setOutputBufferAllocationStrategy(REUSE_INPUT_BUFFER);
        NES_DEBUG("BufferOptimizationPhase: Assign REUSE_INPUT_BUFFER optimization strategy to pipeline.");
        return operatorPipeline;
    }
    if (inputSchema->getSchemaSizeInBytes() >= outputSchema->getSchemaSizeInBytes() && (level == OMIT_OVERFLOW_CHECK_NO_FALLBACK || level == ALL)) {
        // The optimization "omit size check" can be applied.
        emitNode->setOutputBufferAllocationStrategy(OMIT_OVERFLOW_CHECK);
        NES_DEBUG("BufferOptimizationPhase: Assign OMIT_OVERFLOW_CHECK optimization strategy to pipeline.");
        return operatorPipeline;
    }

    // level != NO, but still no optimization can be applied
    NES_DEBUG("BufferOptimizationPhase: Optimization was requested, but no optimization can be applied.");

    return operatorPipeline;
}

}// namespace NES::QueryCompilation