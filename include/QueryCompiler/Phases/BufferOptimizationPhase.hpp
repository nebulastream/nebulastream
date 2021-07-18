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
#ifndef NES_INCLUDE_QUERYCOMPILER_PHASES_BUFFEROPTIMIZATIONPHASE_HPP_
#define NES_INCLUDE_QUERYCOMPILER_PHASES_BUFFEROPTIMIZATIONPHASE_HPP_

#include <QueryCompiler/Operators/OperatorPipeline.hpp>
#include <QueryCompiler/Operators/PipelineQueryPlan.hpp>
#include <QueryCompiler/QueryCompilerForwardDeclaration.hpp>
#include <QueryCompiler/Phases/BufferOptimizationStrategies.hpp>
#include <vector>

namespace NES {
namespace QueryCompilation {

/**
 * @brief This phase lowers a pipeline plan of physical operators into a pipeline plan of generatable operators.
 * The lowering of individual operators is defined by the generatable operator provider to improve extendability.
 */
class BufferOptimizationPhase {
public:
    /**
     * @brief Constructor to create a BufferOptimizationPhase
     */
    explicit BufferOptimizationPhase(BufferOptimizationStrategy desiredOptimization);

    /**
     * @brief Create a BufferOptimizationPhase
     */
    static BufferOptimizationPhasePtr create(BufferOptimizationStrategy desiredOptimization);

    /**
     * @brief Applies the phase on a pipelined query plan. Analyzes every pipeline to see if buffer optimization can be applied.
     * @param pipelined query plan
     * @return PipelineQueryPlanPtr
     */
    PipelineQueryPlanPtr apply(PipelineQueryPlanPtr pipelinedQueryPlan);

    /**
     * @brief Analyzes pipeline to see if buffer optimization can be applied.
     * @param pipeline
     * @return OperatorPipelinePtr
     */
    OperatorPipelinePtr apply(OperatorPipelinePtr pipeline);
private:
    BufferOptimizationStrategy desiredStrategy;
};
}// namespace QueryCompilation
}// namespace NES
#endif//NES_INCLUDE_QUERYCOMPILER_PHASES_BUFFEROPTIMIZATIONPHASE_HPP_
