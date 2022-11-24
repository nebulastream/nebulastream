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
#include <Execution/Pipelines/CompilationPipelineProvider.hpp>
#include <Execution/Pipelines/NautilusExecutablePipelineStage.hpp>
#include <Nodes/Util/Iterators/DepthFirstNodeIterator.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <QueryCompiler/Exceptions/QueryCompilationException.hpp>
#include <QueryCompiler/Operators/ExecutableOperator.hpp>
#include <QueryCompiler/Operators/NautilusPipelineOperator.hpp>
#include <QueryCompiler/Operators/OperatorPipeline.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Joining/PhysicalBatchJoinBuildOperator.hpp>
#include <QueryCompiler/Operators/PipelineQueryPlan.hpp>
#include <QueryCompiler/Phases/NautilusCompilationPase.hpp>
#include <utility>
namespace NES::QueryCompilation {

NautilusCompilationPhase::NautilusCompilationPhase(QueryCompilerOptions::CompilationStrategy compilationStrategy)
    : compilationStrategy(compilationStrategy) {}

std::shared_ptr<NautilusCompilationPhase>
NautilusCompilationPhase::create(QueryCompilerOptions::CompilationStrategy compilationStrategy) {
    return std::make_shared<NautilusCompilationPhase>(compilationStrategy);
}

PipelineQueryPlanPtr NautilusCompilationPhase::apply(PipelineQueryPlanPtr queryPlan) {
    NES_DEBUG("Generate code for query plan " << queryPlan->getQueryId() << " - " << queryPlan->getQuerySubPlanId());
    for (const auto& pipeline : queryPlan->getPipelines()) {
        if (pipeline->isOperatorPipeline()) {
            apply(pipeline);
        }
    }
    return queryPlan;
}

OperatorPipelinePtr NautilusCompilationPhase::apply(OperatorPipelinePtr pipeline) {
    auto& provider = Runtime::Execution::ExecutablePipelineProviderRegistry::getPlugin("PipelineCompiler");

    auto pipelineRoots = pipeline->getQueryPlan()->getRootOperators();
    NES_ASSERT(pipelineRoots.size() == 1, "A pipeline should have a single root operator.");
    auto rootOperator = pipelineRoots[0];

    auto nautilusPipeline = rootOperator->as<NautilusPipelineOperator>();
    auto pipelineStage = provider->create(nautilusPipeline->getNautilusPipeline());
    // we replace the current pipeline operators with an executable operator.
    // this allows us to keep the pipeline structure.
    auto executableOperator = ExecutableOperator::create(std::move(pipelineStage), nautilusPipeline->getOperatorHandlers());
    pipeline->getQueryPlan()->replaceRootOperator(rootOperator, executableOperator);
    return pipeline;
}

}// namespace NES::QueryCompilation
