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
#include <utility>
#include <Execution/Pipelines/CompilationPipelineProvider.hpp>
#include <Execution/Pipelines/CompiledExecutablePipelineStage.hpp>
#include <Nodes/Iterators/DepthFirstNodeIterator.hpp>
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <QueryCompiler/Operators/ExecutableOperator.hpp>
#include <QueryCompiler/Operators/NautilusPipelineOperator.hpp>
#include <QueryCompiler/Operators/OperatorPipeline.hpp>
#include <QueryCompiler/Operators/PipelineQueryPlan.hpp>
#include <QueryCompiler/Phases/NautilusCompilationPase.hpp>
#include <Util/Common.hpp>
#include <ErrorHandling.hpp>
#include <ExecutablePipelineProviderRegistry.hpp>


namespace NES::QueryCompilation
{

NautilusCompilationPhase::NautilusCompilationPhase(const Configurations::QueryCompilerConfiguration& compilerOptions)
    : compilerOptions(compilerOptions)
{
}

std::shared_ptr<PipelineQueryPlan> NautilusCompilationPhase::apply(std::shared_ptr<PipelineQueryPlan> queryPlan)
{
    NES_DEBUG("Generate code for query plan {}", queryPlan->getQueryId());
    for (const auto& pipeline : queryPlan->getPipelines())
    {
        if (pipeline->isOperatorPipeline())
        {
            apply(pipeline);
        }
    }
    return queryPlan;
}

std::string getPipelineProviderIdentifier(const Configurations::QueryCompilerConfiguration& compilerOptions)
{
    switch (compilerOptions.nautilusBackend)
    {
        case Nautilus::Configurations::NautilusBackend::INTERPRETER: {
            return "interpreter";
        };
        case Nautilus::Configurations::NautilusBackend::COMPILER: {
            return "compiler";
        };
        default: {
            INVARIANT(false, "Invalid backend");
        }
    }
}

std::shared_ptr<OperatorPipeline> NautilusCompilationPhase::apply(std::shared_ptr<OperatorPipeline> pipeline)
{
    const auto pipelineRoots = pipeline->getDecomposedQueryPlan()->getRootOperators();
    PRECONDITION(pipelineRoots.size() == 1, "A nautilus pipeline should have a single root operator.");

    const auto& rootOperator = pipelineRoots[0];
    const auto nautilusPipeline = NES::Util::as<NautilusPipelineOperator>(rootOperator);
    nautilus::engine::Options options;
    auto identifier = fmt::format(
        "NautilusCompilation-{}-{}-{}",
        pipeline->getDecomposedQueryPlan()->getWorkerId(),
        pipeline->getDecomposedQueryPlan()->getQueryId(),
        pipeline->getPipelineId());

    /// enable dump to console or file if the compiler options are set
    options.setOption(
        "toConsole",
        compilerOptions.dumpMode == Configurations::DumpMode::CONSOLE
            || compilerOptions.dumpMode == Configurations::DumpMode::FILE_AND_CONSOLE);
    options.setOption(
        "toFile",
        compilerOptions.dumpMode == Configurations::DumpMode::FILE
            || compilerOptions.dumpMode == Configurations::DumpMode::FILE_AND_CONSOLE);

    auto providerName = getPipelineProviderIdentifier(compilerOptions);
    auto providerArguments = Runtime::Execution::ExecutablePipelineProviderRegistryArguments{};
    if (const auto provider = Runtime::Execution::ExecutablePipelineProviderRegistry::instance().create(providerName, providerArguments))
    {
        auto pipelineStage
            = provider.value()->create(nautilusPipeline->getNautilusPipeline(), nautilusPipeline->getOperatorHandlers(), options);
        /// we replace the current pipeline operators with an executable operator.
        /// this allows us to keep the pipeline structure.
        const auto executableOperator = ExecutableOperator::create(std::move(pipelineStage), nautilusPipeline->getOperatorHandlers());
        pipeline->getDecomposedQueryPlan()->replaceRootOperator(rootOperator, executableOperator);
        return pipeline;
    }
    throw UnknownExecutablePipelineProviderType(fmt::format("ExecutablePipelineProvider plugin of type: {} not registered.", providerName));
}

}
