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
#pragma once

#include <vector>
#include <QueryCompiler/Operators/OperatorPipeline.hpp>
#include <QueryCompiler/Operators/PipelineQueryPlan.hpp>
#include <QueryCompiler/Phases/OutputBufferAllocationStrategies.hpp>
#include <QueryCompiler/QueryCompilerOptions.hpp>

namespace NES::QueryCompilation
{
class BufferOptimizationPhase;
using BufferOptimizationPhasePtr = std::shared_ptr<BufferOptimizationPhase>;

/// @brief This phase scans all pipelines and determines if the OutputBufferOptimizationLevel (level) requested by the user can be applied.
/// It then notes the correct OutputBufferAllocationStrategy in the Emit operator of the pipeline.
class BufferOptimizationPhase
{
public:
    explicit BufferOptimizationPhase(OutputBufferOptimizationLevel level);
    static BufferOptimizationPhasePtr create(OutputBufferOptimizationLevel level);

    PipelineQueryPlanPtr apply(PipelineQueryPlanPtr pipelinedQueryPlan);
    OperatorPipelinePtr apply(OperatorPipelinePtr pipeline);

private:
    [[maybe_unused]] OutputBufferOptimizationLevel level;
};
}
