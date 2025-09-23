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


#include <QueryCompiler.hpp>

#include <memory>

#include <Configuration/WorkerConfiguration.hpp>
#include <Phases/LowerToCompiledQueryPlanPhase.hpp>
#include <Phases/PipelineBuilder/PipelineBuilder.hpp>
#include <Util/DumpMode.hpp>
#include <CompiledQueryPlan.hpp>
#include <ErrorHandling.hpp>

namespace NES::QueryCompilation
{

QueryCompiler::QueryCompiler() = default;

/// This phase should be as dumb as possible and not further decisions should be made here.
std::unique_ptr<CompiledQueryPlan> QueryCompiler::compileQuery(std::unique_ptr<QueryCompilationRequest> request)
{
    auto pipelineBuilder = std::make_unique<PipelineBuilder>();
    auto lowerToCompiledQueryPlanPhase = LowerToCompiledQueryPlanPhase(request->dumpCompilationResult);

    try
    {
        /// build the query plan using the new FSM
        auto pipelinedQueryPlan = pipelineBuilder->build(request->queryPlan);
        return lowerToCompiledQueryPlanPhase.apply(pipelinedQueryPlan);
    }
    catch (const std::exception& e)
    {
        NES_ERROR("Failed to build and apply PipelinedQueryPlan: . Error: {}.", e.what());
        throw;
    }
}
}
