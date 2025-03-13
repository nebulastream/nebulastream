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
#include <memory>
#include <string>
#include <utility>
#include <Pipelines/CompilationPipelineProvider.hpp>
#include <Pipelines/CompiledExecutablePipelineStage.hpp>
#include <Phases/NautilusCompilationPhase.hpp>
#include <Util/Common.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <ExecutableOperator.hpp>
#include <ExecutablePipelineProviderRegistry.hpp>
#include <PipelinedQueryPlan.hpp>
#include <Pipeline.hpp>
#include <options.hpp>

namespace NES::QueryCompilation::NautilusCompilationPhase
{

void apply(std::unique_ptr<OperatorPipeline> pipeline)
{
    const auto& rootOperator = pipeline->rootOperator;

    nautilus::engine::Options options;
    auto identifier = fmt::format(
        "NautilusCompilation-{}",
        pipeline->pipelineId);
    options.setOption("toConsole", true);
    options.setOption("toFile", true);

    auto providerArguments = ExecutablePipelineProviderRegistryArguments{};
    if (const auto provider = ExecutablePipelineProviderRegistry::instance().create(magic_enum::enum_type_name<Pipeline::ProviderType>(pipeline->providerType), providerArguments))
    {
        auto pipelineStage
            = provider.value()->create(pipeline, options);
        /// we replace the current pipeline operators with an executable operator.
        /// this allows us to keep the pipeline structure.
        const auto executableOperator = ExecutableOperator::create(std::move(pipelineStage), pipeline->operatorHandlers);
        ///pipeline.rootOperator.replaceRootOperator(rootOperator, executableOperator);
        pipeline->rootOperator = executableOperator;

        /// TODO do the actual compilation
        return;
    }
    throw UnknownExecutablePipelineProviderType("ExecutablePipelineProvider plugin of type: {} not registered.", "providerName");
}

std::unique_ptr<PipelinedQueryPlan> apply(std::unique_ptr<PipelinedQueryPlan> plan)
{
    for (auto& pipeline : plan->pipelines)
    {
        if (Util::uniquePtrInstanceOf<Pipeline, OperatorPipeline>(pipeline))
        {
            apply(Util::unique_ptr_dynamic_cast<OperatorPipeline>(std::move(pipeline)));
        }
    }
    return plan;
}

}