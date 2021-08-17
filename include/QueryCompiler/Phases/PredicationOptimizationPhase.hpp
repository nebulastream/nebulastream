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
#ifndef NES_INCLUDE_QUERYCOMPILER_PHASES_PREDICATIONOPTIMIZATIONPHASE_HPP_
#define NES_INCLUDE_QUERYCOMPILER_PHASES_PREDICATIONOPTIMIZATIONPHASE_HPP_

#include <QueryCompiler/Operators/OperatorPipeline.hpp>
#include <QueryCompiler/Operators/PipelineQueryPlan.hpp>
#include <QueryCompiler/QueryCompilerForwardDeclaration.hpp>
#include <QueryCompiler/QueryCompilerOptions.hpp>
#include <vector>

namespace NES {
namespace QueryCompilation {

/**
 * @brief This phase scans all pipelines. If enabled, it replaces all suited filter operator with equivalent predicated filter operators.
 * It then configures the following emit operators.
 */
class PredicationOptimizationPhase {
  public:
    /**
     * @brief Constructor to create a PredicationOptimizationPhase
     */
    explicit PredicationOptimizationPhase(bool predicationEnabled);

    /**
     * @brief Create a PredicationOptimizationPhase
     */
    static PredicationOptimizationPhasePtr create(bool predicationEnabled);

    /**
     * @brief Applies the phase on a pipelined query plan. Analyzes every pipeline to see if predication optimization can be applied.
     * @param pipelined query plan
     * @return PipelineQueryPlanPtr
     */
    PipelineQueryPlanPtr apply(PipelineQueryPlanPtr pipelinedQueryPlan);

    /**
     * @brief Analyzes pipeline to see if predication optimization can be applied.
     * @param pipeline
     * @return OperatorPipelinePtr
     */
    OperatorPipelinePtr apply(OperatorPipelinePtr pipeline);

  private:
    bool predicationEnabled;
};
}// namespace QueryCompilation
}// namespace NES
#endif//NES_INCLUDE_QUERYCOMPILER_PHASES_PREDICATIONOPTIMIZATIONPHASE_HPP_
