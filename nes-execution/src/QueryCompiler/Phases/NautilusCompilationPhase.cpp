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
#include <Execution/Pipelines/ExecutablePipelineProviderRegistry.hpp>
#include <Nodes/Iterators/DepthFirstNodeIterator.hpp>
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <QueryCompiler/Operators/ExecutableOperator.hpp>
#include <QueryCompiler/Operators/NautilusPipelineOperator.hpp>
#include <QueryCompiler/Operators/OperatorPipeline.hpp>
#include <QueryCompiler/Operators/PipelineQueryPlan.hpp>
#include <QueryCompiler/Phases/NautilusCompilationPase.hpp>
#include <Util/Common.hpp>
#include <ErrorHandling.hpp>


namespace NES::QueryCompilation
{

NautilusCompilationPhase::NautilusCompilationPhase(const std::shared_ptr<QueryCompilerOptions>& compilerOptions)
    : compilerOptions(compilerOptions)
{
}

PipelineQueryPlanPtr NautilusCompilationPhase::apply(PipelineQueryPlanPtr queryPlan)
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

std::string getPipelineProviderIdentifier(const std::shared_ptr<QueryCompilerOptions>& compilerOptions)
{
    switch (compilerOptions->nautilusBackend)
    {
        case NautilusBackend::INTERPRETER: {
            return "PipelineInterpreter";
        };
        case NautilusBackend::COMPILER: {
            return "PipelineCompiler";
        };
        default: {
            INVARIANT(false, "Invalid backend");
        }
    }
}

OperatorPipelinePtr NautilusCompilationPhase::apply(OperatorPipelinePtr pipeline)
{
    auto pipelineRoots = pipeline->getDecomposedQueryPlan()->getRootOperators();
    PRECONDITION(pipelineRoots.size() == 1, "A pipeline should have a single root operator.");

    auto rootOperator = pipelineRoots[0];
    auto nautilusPipeline = NES::Util::as<NautilusPipelineOperator>(rootOperator);
    nautilus::engine::Options options;
    auto identifier = fmt::format(
        "NautilusCompilation-{}-{}-{}",
        pipeline->getDecomposedQueryPlan()->getQueryId(),
        pipeline->getDecomposedQueryPlan()->getQueryId(),
        pipeline->getPipelineId());

    /// enable dump to console or file if the compiler options are set
    options.setOption(
        "toConsole", compilerOptions->dumpMode == DumpMode::CONSOLE || compilerOptions->dumpMode == DumpMode::FILE_AND_CONSOLE);
    options.setOption("toFile", compilerOptions->dumpMode == DumpMode::FILE || compilerOptions->dumpMode == DumpMode::FILE_AND_CONSOLE);

    auto providerName = getPipelineProviderIdentifier(compilerOptions);
    if (auto provider = Runtime::Execution::ExecutablePipelineProviderRegistry::instance().create(providerName))
    {
        auto pipelineStage
            = provider.value()->create(nautilusPipeline->getNautilusPipeline(), nautilusPipeline->getOperatorHandlers(), options);
        /// we replace the current pipeline operators with an executable operator.
        /// this allows us to keep the pipeline structure.
        auto executableOperator = ExecutableOperator::create(std::move(pipelineStage), nautilusPipeline->getOperatorHandlers());
        pipeline->getDecomposedQueryPlan()->replaceRootOperator(rootOperator, executableOperator);
        return pipeline;
    }
    throw UnknownExecutablePipelineProviderType(fmt::format("ExecutablePipelineProvider plugin of type: {} not registered.", providerName));
}

}
