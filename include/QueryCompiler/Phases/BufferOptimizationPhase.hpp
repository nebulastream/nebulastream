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
#ifndef NES_INCLUDE_QUERY_COMPILER_PHASES_BUFFER_OPTIMIZATION_PHASE_HPP_
#define NES_INCLUDE_QUERY_COMPILER_PHASES_BUFFER_OPTIMIZATION_PHASE_HPP_

#include <QueryCompiler/Operators/OperatorPipeline.hpp>
#include <QueryCompiler/Operators/PipelineQueryPlan.hpp>
#include <QueryCompiler/Phases/OutputBufferAllocationStrategies.hpp>
#include <QueryCompiler/QueryCompilerForwardDeclaration.hpp>
#include <QueryCompiler/QueryCompilerOptions.hpp>
#include <vector>

namespace NES {
namespace QueryCompilation {

/**
 * @brief This phase scans all pipelines and determines if the OutputBufferOptimizationLevel (level) requested by the user can be applied.
 * It then notes the correct OutputBufferAllocationStrategy in the Emit operator of the pipeline.
 */
class BufferOptimizationPhase {
  public:
    /**
     * @brief Constructor to create a BufferOptimizationPhase
     */
    explicit BufferOptimizationPhase(QueryCompilerOptions::OutputBufferOptimizationLevel level);

    /**
     * @brief Create a BufferOptimizationPhase
     */
    static BufferOptimizationPhasePtr create(QueryCompilerOptions::OutputBufferOptimizationLevel level);

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
    QueryCompilerOptions::OutputBufferOptimizationLevel level;
    /**
     * @brief Indicates if the input of this pipeline is read only.
     * For instance, if the input is shared by a sibling pipeline.
     * @param pipeline
     * @return true if pipeline input is read only.
     */
    bool isReadOnlyInput(OperatorPipelinePtr pipeline);
};
}// namespace QueryCompilation
}// namespace NES
#endif  // NES_INCLUDE_QUERY_COMPILER_PHASES_BUFFER_OPTIMIZATION_PHASE_HPP_
