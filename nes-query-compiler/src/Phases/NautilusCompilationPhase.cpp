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
#include <string>
#include <utility>
#include <Pipelines/CompilationPipelineProvider.hpp>
#include <Pipelines/CompiledExecutablePipelineStage.hpp>
#include <Phases/NautilusCompilationPhase.hpp>
#include <Util/Common.hpp>
#include <ErrorHandling.hpp>
#include <ExecutableOperator.hpp>
//#include <ExecutablePipelineProviderRegistry.hpp>
#include <PipelinedQueryPlan.hpp>

namespace NES::QueryCompilation::NautilusCompilationPhase
{

// We expected that the operator pipeline root include a NautilusOperator...
std::shared_ptr<Pipeline> apply(std::shared_ptr<Pipeline>)
{
    //const auto pipelineRoots = pipeline->getDecomposedQueryPlan()->getRootOperators();
    //PRECONDITION(pipelineRoots.size() == 1, "A nautilus pipeline should have a single root operator.");

    //const auto& rootOperator = pipelineRoots[0];
    //nautilus::engine::Options options;
    //auto identifier = fmt::format(
    //    "NautilusCompilation-{}-{}-{}",
    //    pipeline->getDecomposedQueryPlan()->getWorkerId(),
    //    pipeline->getDecomposedQueryPlan()->getQueryId(),
    //    pipeline->getPipelineId());

    /// enable dump to console or file if the compiler options are set
    //options.setOption(
    //    "toConsole",
    //    compilerOptions.dumpMode == NES::Configurations::DumpMode::CONSOLE
    //        || compilerOptions.dumpMode == NES::Configurations::DumpMode::FILE_AND_CONSOLE);
    //options.setOption(
    //    "toFile",
    //    compilerOptions.dumpMode == NES::Configurations::DumpMode::FILE
    //        || compilerOptions.dumpMode == NES::Configurations::DumpMode::FILE_AND_CONSOLE);
/*
    auto providerArguments = ExecutablePipelineProviderRegistryArguments{};
    if (const auto provider = ExecutablePipelineProviderRegistry::instance().create(pipeline->getPipelineProviderType(), providerArguments))
    {
        auto pipelineStage
            = provider.value()->create(pipeline, options);
        /// we replace the current pipeline operators with an executable operator.
        /// this allows us to keep the pipeline structure.
        const auto executableOperator = ExecutableOperator::create(std::move(pipelineStage), pipeline->operatorHandlers);
        pipeline->getQueryPlan()->replaceRootOperator(rootOperator, executableOperator);

        /// TODO do the actual compilation
        return pipeline;
    }*/
    throw UnknownExecutablePipelineProviderType("ExecutablePipelineProvider plugin of type: {} not registered.", "providerName");
}

std::shared_ptr<PipelinedQueryPlan> apply(std::shared_ptr<PipelinedQueryPlan> plan)
{
    for (const auto& pipeline : plan->pipelines)
    {
        if (Util::instanceOf<OperatorPipeline>(pipeline))
        {
            apply(pipeline);
        }
    }
    return plan;
}


}