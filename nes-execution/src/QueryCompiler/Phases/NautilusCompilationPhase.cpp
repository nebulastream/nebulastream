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
#include <Execution/Pipelines/CompiledExecutablePipelineStage.hpp>
#include <Nautilus/NautilusBackend.hpp>
#include <Nodes/Iterators/DepthFirstNodeIterator.hpp>
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <QueryCompiler/Configurations/Enums/DumpMode.hpp>
#include <QueryCompiler/Configurations/QueryCompilerConfiguration.hpp>
#include <QueryCompiler/Operators/ExecutableOperator.hpp>
#include <QueryCompiler/Operators/NautilusPipelineOperator.hpp>
#include <QueryCompiler/Operators/OperatorPipeline.hpp>
#include <QueryCompiler/Operators/PipelineQueryPlan.hpp>
#include <QueryCompiler/Phases/NautilusCompilationPase.hpp>
#include <Util/Common.hpp>
#include <ErrorHandling.hpp>

namespace NES::QueryCompilation
{

NautilusCompilationPhase::NautilusCompilationPhase(Configurations::QueryCompilerConfiguration compilerOptions)
    : compilerOptions(std::move(compilerOptions))
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

nautilus::engine::Options setNautilusOptions(const Configurations::QueryCompilerConfiguration& compilerOptions)
{
    nautilus::engine::Options options;

    /// enable dump to console or file if the compiler options are set
    options.setOption(
        "toConsole",
        compilerOptions.dumpMode == Configurations::DumpMode::CONSOLE
            || compilerOptions.dumpMode == Configurations::DumpMode::FILE_AND_CONSOLE);
    options.setOption(
        "toFile",
        compilerOptions.dumpMode == Configurations::DumpMode::FILE
            || compilerOptions.dumpMode == Configurations::DumpMode::FILE_AND_CONSOLE);


    switch (compilerOptions.nautilusBackend)
    {
        case Nautilus::Configurations::NautilusBackend::INTERPRETER: {
            options.setOption("engine.Compilation", false);
            break;
        }
        case Nautilus::Configurations::NautilusBackend::COMPILER: {
            options.setOption("engine.Compilation", true);
            break;
        }
        default: {
            INVARIANT(false, "Invalid backend");
        }
    }

    return options;
}

std::shared_ptr<OperatorPipeline> NautilusCompilationPhase::apply(std::shared_ptr<OperatorPipeline> pipeline)
{
    const auto pipelineRoots = pipeline->getDecomposedQueryPlan()->getRootOperators();
    PRECONDITION(pipelineRoots.size() == 1, "A nautilus pipeline should have a single root operator.");

    const auto& rootOperator = pipelineRoots[0];
    const auto nautilusPipeline = NES::Util::as<NautilusPipelineOperator>(rootOperator);

    /// Setting the options for the nautilus backend based on the compiler options
    const auto nautilusOptions = setNautilusOptions(compilerOptions);
    auto pipelineStage = std::make_unique<Runtime::Execution::CompiledExecutablePipelineStage>(
        nautilusPipeline->getNautilusPipeline(), nautilusPipeline->getOperatorHandlers(), nautilusOptions);

    /// we replace the current pipeline operators with an executable operator.
    /// this allows us to keep the pipeline structure.
    const auto executableOperator = ExecutableOperator::create(std::move(pipelineStage), nautilusPipeline->getOperatorHandlers());
    pipeline->getDecomposedQueryPlan()->replaceRootOperator(rootOperator, executableOperator);
    return pipeline;
}

}
