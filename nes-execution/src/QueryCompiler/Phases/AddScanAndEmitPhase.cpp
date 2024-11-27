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

#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <QueryCompiler/Operators/OperatorPipeline.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/AbstractEmitOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/AbstractScanOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalBinaryOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalEmitOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalScanOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalUnaryOperator.hpp>
#include <QueryCompiler/Operators/PipelineQueryPlan.hpp>
#include <QueryCompiler/Phases/AddScanAndEmitPhase.hpp>
#include <Util/Common.hpp>
#include <ErrorHandling.hpp>


namespace NES::QueryCompilation
{

AddScanAndEmitPhasePtr AddScanAndEmitPhase::create()
{
    return std::make_shared<AddScanAndEmitPhase>();
}

PipelineQueryPlanPtr AddScanAndEmitPhase::apply(PipelineQueryPlanPtr pipelineQueryPlan)
{
    for (const auto& pipeline : pipelineQueryPlan->getPipelines())
    {
        if (pipeline->isOperatorPipeline())
        {
            process(pipeline);
        }
    }
    return pipelineQueryPlan;
}

OperatorPipelinePtr AddScanAndEmitPhase::process(OperatorPipelinePtr pipeline)
{
    auto decomposedQueryPlan = pipeline->getDecomposedQueryPlan();
    auto pipelineRootOperators = decomposedQueryPlan->getRootOperators();
    PRECONDITION(!pipelineRootOperators.empty(), "A pipeline should have at least one root operator");

    /// insert buffer scan operator to the pipeline root if necessary
    auto rootOperator = pipelineRootOperators[0];
    if (!NES::Util::instanceOf<PhysicalOperators::AbstractScanOperator>(rootOperator))
    {
        PRECONDITION(
            NES::Util::instanceOf<PhysicalOperators::PhysicalUnaryOperator>(rootOperator),
            "Pipeline root should be a unary operator but was: {}",
            *rootOperator);
        auto unaryRoot = NES::Util::as<PhysicalOperators::PhysicalUnaryOperator>(rootOperator);
        auto newScan = PhysicalOperators::PhysicalScanOperator::create(unaryRoot->getInputSchema());
        pipeline->prependOperator(newScan);
    }

    /// insert emit buffer operator if necessary
    auto pipelineLeafOperators = rootOperator->getAllLeafNodes();
    for (const auto& leaf : pipelineLeafOperators)
    {
        auto leafOperator = NES::Util::as<Operator>(leaf);
        if (!NES::Util::instanceOf<PhysicalOperators::AbstractEmitOperator>(leafOperator))
        {
            auto emitOperator = PhysicalOperators::PhysicalEmitOperator::create(leafOperator->getOutputSchema());
            leafOperator->addChild(emitOperator);
        }
    }
    return pipeline;
}

}
