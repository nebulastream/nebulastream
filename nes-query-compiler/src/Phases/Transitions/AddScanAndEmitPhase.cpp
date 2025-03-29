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
#include <Plans/PipelineQueryPlan.hpp>
#include <Phases/Translations/AddScanAndEmitPhase.hpp>
#include <ScanPhysicalOperator.hpp>
#include <ErrorHandling.hpp>


namespace NES::QueryCompilation
{

std::shared_ptr<AddScanAndEmitPhase> AddScanAndEmitPhase::create()
{
    return std::make_shared<AddScanAndEmitPhase>();
}

std::shared_ptr<PipelineQueryPlan> AddScanAndEmitPhase::apply(std::shared_ptr<PipelineQueryPlan> pipelineQueryPlan)
{
    for (const auto& pipeline : pipelineQueryPlan->pipelines)
    {
        if (pipeline->isOperatorPipeline())
        {
            process(pipeline);
        }
    }
    return pipelineQueryPlan;
}

std::shared_ptr<OperatorPipeline> AddScanAndEmitPhase::process(std::shared_ptr<OperatorPipeline> pipeline)
{
    const auto queryPlan = pipeline->getQueryPlan();
    const auto pipelineRootOperators = queryPlan->getRootOperators();
    PRECONDITION(!pipelineRootOperators.empty(), "A pipeline should have at least one root operator");

    /// insert buffer scan operator to the pipeline root if necessary
    const auto& rootOperator = pipelineRootOperators[0];
    if (!NES::Util::instanceOf<ScanPhysicalOperator>(rootOperator))
    {
        PRECONDITION(
            NES::Util::instanceOf<UnaryPhysicalOperator>(rootOperator),
            "Pipeline root should be a unary operator but was: {}",
            *rootOperator);
        const auto unaryRoot = NES::Util::as<UnaryPhysicalOperator>(rootOperator);
        const auto newScan = ScanPhysicalOperator::create(unaryRoot->getInputSchema());
        pipeline->prependOperator(newScan);
    }

    /// insert emit buffer operator if necessary
    auto pipelineLeafOperators = rootOperator->getAllLeafNodes();
    for (const auto& leaf : pipelineLeafOperators)
    {
        auto leafOperator = NES::Util::as<Operator>(leaf);
        if (!NES::Util::instanceOf<EmitPhysicalOperator>(leafOperator))
        {
            auto emitOperator = EmitPhysicalOperator(leafOperator->getOutputSchema());
            leafOperator->addChild(emitOperator);
        }
    }
    return pipeline;
}

}