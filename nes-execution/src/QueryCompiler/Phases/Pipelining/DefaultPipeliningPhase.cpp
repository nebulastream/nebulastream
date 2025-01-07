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

#include <Operators/LogicalOperators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/LogicalOperators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <QueryCompiler/Operators/OperatorPipeline.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalDemultiplexOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalUnionOperator.hpp>
#include <QueryCompiler/Operators/PipelineQueryPlan.hpp>
#include <QueryCompiler/Phases/Pipelining/DefaultPipeliningPhase.hpp>
#include <QueryCompiler/Phases/Pipelining/OperatorFusionPolicy.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>

namespace NES::QueryCompilation
{

DefaultPipeliningPhase::DefaultPipeliningPhase(OperatorFusionPolicyPtr operatorFusionPolicy)
    : operatorFusionPolicy(std::move(operatorFusionPolicy))
{
}

PipeliningPhasePtr DefaultPipeliningPhase::create(const OperatorFusionPolicyPtr& operatorFusionPolicy)
{
    return std::make_shared<DefaultPipeliningPhase>(operatorFusionPolicy);
}

PipelineQueryPlanPtr DefaultPipeliningPhase::apply(DecomposedQueryPlanPtr decomposedQueryPlan)
{
    /// splits the query plan of physical operators in pipelines
    NES_DEBUG("Pipeline: query id: {}", decomposedQueryPlan->getQueryId());
    std::map<std::shared_ptr<Operator>, OperatorPipelinePtr> pipelineOperatorMap;
    auto pipelinePlan = PipelineQueryPlan::create(decomposedQueryPlan->getQueryId());
    for (const auto& sinkOperator : decomposedQueryPlan->getRootOperators())
    {
        /// create a new pipeline for each sink
        auto pipeline = OperatorPipeline::createSinkPipeline();
        pipeline->prependOperator(sinkOperator->copy());
        pipelinePlan->addPipeline(pipeline);
        process(pipelinePlan, pipelineOperatorMap, pipeline, sinkOperator);
    }
    return pipelinePlan;
}

void DefaultPipeliningPhase::processMultiplex(
    const PipelineQueryPlanPtr& pipelinePlan,
    std::map<std::shared_ptr<Operator>, OperatorPipelinePtr>& pipelineOperatorMap,
    OperatorPipelinePtr currentPipeline,
    const PhysicalOperators::PhysicalOperatorPtr& currentOperator)
{
    /// if the current pipeline has no operators we will remove it, because we want to omit empty pipelines
    if (!currentPipeline->hasOperators())
    {
        auto successorPipeline = currentPipeline->getSuccessors()[0];
        pipelinePlan->removePipeline(currentPipeline);
        currentPipeline = successorPipeline;
    }
    /// for all children operators add a new pipeline
    for (const auto& node : currentOperator->getChildren())
    {
        auto newPipeline = OperatorPipeline::create();
        pipelinePlan->addPipeline(newPipeline);
        newPipeline->addSuccessor(currentPipeline);
        process(pipelinePlan, pipelineOperatorMap, newPipeline, NES::Util::as<PhysicalOperators::PhysicalOperator>(node));
    }
}

void DefaultPipeliningPhase::processDemultiplex(
    const PipelineQueryPlanPtr& pipelinePlan,
    std::map<std::shared_ptr<Operator>, OperatorPipelinePtr>& pipelineOperatorMap,
    OperatorPipelinePtr currentPipeline,
    const PhysicalOperators::PhysicalOperatorPtr& currentOperator)
{
    /// if the current pipeline has no operators we will remove it, because we want to omit empty pipelines
    if (!currentPipeline->hasOperators())
    {
        auto successorPipeline = currentPipeline->getSuccessors()[0];
        pipelinePlan->removePipeline(currentPipeline);
        currentPipeline = successorPipeline;
    }
    /// check if current operator is already part of a pipeline.
    /// if yes lookup the pipeline fom the map. If not create a new one
    if (pipelineOperatorMap.find(currentOperator) != pipelineOperatorMap.end())
    {
        pipelineOperatorMap[currentOperator]->addSuccessor(currentPipeline);
    }
    else
    {
        auto newPipeline = OperatorPipeline::create();
        pipelinePlan->addPipeline(newPipeline);
        newPipeline->addSuccessor(currentPipeline);
        process(pipelinePlan, pipelineOperatorMap, newPipeline, NES::Util::as<Operator>(currentOperator->getChildren()[0]));
        pipelineOperatorMap[currentOperator] = newPipeline;
    }
}

void DefaultPipeliningPhase::processPipelineBreakerOperator(
    const PipelineQueryPlanPtr& pipelinePlan,
    std::map<std::shared_ptr<Operator>, OperatorPipelinePtr>& pipelineOperatorMap,
    const OperatorPipelinePtr& currentPipeline,
    const PhysicalOperators::PhysicalOperatorPtr& currentOperator)
{
    /// for pipeline breakers we create a new pipeline
    currentPipeline->prependOperator(NES::Util::as<PhysicalOperators::PhysicalOperator>(currentOperator)->copy());

    for (const auto& node : currentOperator->getChildren())
    {
        auto newPipeline = OperatorPipeline::create();
        pipelinePlan->addPipeline(newPipeline);
        newPipeline->addSuccessor(currentPipeline);
        process(pipelinePlan, pipelineOperatorMap, newPipeline, NES::Util::as<Operator>(node));
    }
}

void DefaultPipeliningPhase::processFusibleOperator(
    const PipelineQueryPlanPtr& pipelinePlan,
    std::map<std::shared_ptr<Operator>, OperatorPipelinePtr>& pipelineOperatorMap,
    const OperatorPipelinePtr& currentPipeline,
    const PhysicalOperators::PhysicalOperatorPtr& currentOperator)
{
    /// for operator we can fuse, we just append them to the current pipeline.
    currentPipeline->prependOperator(currentOperator->copy());
    for (const auto& node : currentOperator->getChildren())
    {
        process(pipelinePlan, pipelineOperatorMap, currentPipeline, NES::Util::as<Operator>(node));
    }
}

void DefaultPipeliningPhase::processSink(
    const PipelineQueryPlanPtr& pipelinePlan,
    std::map<std::shared_ptr<Operator>, OperatorPipelinePtr>& pipelineOperatorMap,
    const OperatorPipelinePtr& currentPipeline,
    const SinkLogicalOperator& currentOperator)
{
    for (const auto& child : currentOperator.getChildren())
    {
        auto cp = OperatorPipeline::create();
        pipelinePlan->addPipeline(cp);
        cp->addSuccessor(currentPipeline);
        process(pipelinePlan, pipelineOperatorMap, cp, NES::Util::as<Operator>(child));
    }
}

void DefaultPipeliningPhase::processSource(
    const PipelineQueryPlanPtr& pipelinePlan,
    std::map<std::shared_ptr<Operator>, OperatorPipelinePtr>&,
    OperatorPipelinePtr currentPipeline,
    const std::shared_ptr<SourceDescriptorLogicalOperator>& sourceOperator)
{
    /// Source operators will always be part of their own pipeline.
    if (currentPipeline->hasOperators())
    {
        auto newPipeline = OperatorPipeline::create();
        pipelinePlan->addPipeline(newPipeline);
        newPipeline->addSuccessor(currentPipeline);
        currentPipeline = newPipeline;
    }
    currentPipeline->setType(OperatorPipeline::Type::SourcePipelineType);
    currentPipeline->prependOperator(sourceOperator);
}

void DefaultPipeliningPhase::process(
    const PipelineQueryPlanPtr& pipelinePlan,
    std::map<std::shared_ptr<Operator>, OperatorPipelinePtr>& pipelineOperatorMap,
    const OperatorPipelinePtr& currentPipeline,
    const std::shared_ptr<Operator>& currentOperator)
{
    PRECONDITION(
        Util::instanceOf<PhysicalOperators::PhysicalOperator>(currentOperator)
            or Util::instanceOf<SourceDescriptorLogicalOperator>(currentOperator) or Util::instanceOf<SinkLogicalOperator>(currentOperator),
        "expected a PhysicalOperator, but got {}",
        *currentOperator);

    /// Depending on the operator we apply different pipelining strategies
    if (Util::instanceOf<SourceDescriptorLogicalOperator>(currentOperator))
    {
        processSource(pipelinePlan, pipelineOperatorMap, currentPipeline, Util::as<SourceDescriptorLogicalOperator>(currentOperator));
    }
    else if (Util::instanceOf<SinkLogicalOperator>(currentOperator))
    {
        processSink(pipelinePlan, pipelineOperatorMap, currentPipeline, Util::as<const SinkLogicalOperator>(*currentOperator));
    }
    else if (Util::instanceOf<PhysicalOperators::PhysicalUnionOperator>(currentOperator))
    {
        processMultiplex(
            pipelinePlan, pipelineOperatorMap, currentPipeline, Util::as<PhysicalOperators::PhysicalOperator>(currentOperator));
    }
    else if (Util::instanceOf<PhysicalOperators::PhysicalDemultiplexOperator>(currentOperator))
    {
        processDemultiplex(
            pipelinePlan, pipelineOperatorMap, currentPipeline, Util::as<PhysicalOperators::PhysicalOperator>(currentOperator));
    }
    else if (operatorFusionPolicy->isFusible(Util::as<PhysicalOperators::PhysicalOperator>(currentOperator)))
    {
        processFusibleOperator(
            pipelinePlan, pipelineOperatorMap, currentPipeline, Util::as<PhysicalOperators::PhysicalOperator>(currentOperator));
    }
    else
    {
        processPipelineBreakerOperator(
            pipelinePlan, pipelineOperatorMap, currentPipeline, Util::as<PhysicalOperators::PhysicalOperator>(currentOperator));
    }
}
}
