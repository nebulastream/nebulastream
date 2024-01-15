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
#include <QueryCompiler/Exceptions/QueryCompilationException.hpp>
#include <QueryCompiler/Operators/OperatorPipeline.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalDemultiplexOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalSinkOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalSourceOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalUnionOperator.hpp>
#include <QueryCompiler/Operators/PipelineQueryPlan.hpp>
#include <QueryCompiler/Phases/Pipelining/DefaultPipeliningPhase.hpp>
#include <QueryCompiler/Phases/Pipelining/OperatorFusionPolicy.hpp>
#include <Util/Logger/Logger.hpp>
#include <utility>

namespace NES::QueryCompilation {

DefaultPipeliningPhase::DefaultPipeliningPhase(OperatorFusionPolicyPtr operatorFusionPolicy)
    : operatorFusionPolicy(std::move(operatorFusionPolicy)) {}

PipeliningPhasePtr DefaultPipeliningPhase::create(const OperatorFusionPolicyPtr& operatorFusionPolicy) {
    return std::make_shared<DefaultPipeliningPhase>(operatorFusionPolicy);
}

PipelineQueryPlanPtr DefaultPipeliningPhase::apply(DecomposedQueryPlanPtr decomposedQueryPlan) {

    // splits the query plan of physical operators in pipelines
    NES_DEBUG("Pipeline: query id: {} - {}",
              decomposedQueryPlan->getSharedQueryId(),
              decomposedQueryPlan->getDecomposedQueryPlanId());
    std::map<OperatorNodePtr, OperatorPipelinePtr> pipelineOperatorMap;
    auto pipelinePlan =
        PipelineQueryPlan::create(decomposedQueryPlan->getSharedQueryId(), decomposedQueryPlan->getDecomposedQueryPlanId());
    for (const auto& sinkOperators : decomposedQueryPlan->getRootOperators()) {
        // create a new pipeline for each sink
        auto pipeline = OperatorPipeline::createSinkPipeline();
        pipeline->prependOperator(sinkOperators->copy());
        pipelinePlan->addPipeline(pipeline);
        processSink(pipelinePlan, pipelineOperatorMap, pipeline, sinkOperators->as<PhysicalOperators::PhysicalOperator>());
    }
    return pipelinePlan;
}

void DefaultPipeliningPhase::processMultiplex(const PipelineQueryPlanPtr& pipelinePlan,
                                              std::map<OperatorNodePtr, OperatorPipelinePtr>& pipelineOperatorMap,
                                              OperatorPipelinePtr currentPipeline,
                                              const PhysicalOperators::PhysicalOperatorPtr& currentOperator) {
    // if the current pipeline has no operators we will remove it, because we want to omit empty pipelines
    if (!currentPipeline->hasOperators()) {
        auto successorPipeline = currentPipeline->getSuccessors()[0];
        pipelinePlan->removePipeline(currentPipeline);
        currentPipeline = successorPipeline;
    }
    // for all children operators add a new pipeline
    for (const auto& node : currentOperator->getChildren()) {
        auto newPipeline = OperatorPipeline::create();
        pipelinePlan->addPipeline(newPipeline);
        newPipeline->addSuccessor(currentPipeline);
        process(pipelinePlan, pipelineOperatorMap, newPipeline, node->as<PhysicalOperators::PhysicalOperator>());
    }
}

void DefaultPipeliningPhase::processDemultiplex(const PipelineQueryPlanPtr& pipelinePlan,
                                                std::map<OperatorNodePtr, OperatorPipelinePtr>& pipelineOperatorMap,
                                                OperatorPipelinePtr currentPipeline,
                                                const PhysicalOperators::PhysicalOperatorPtr& currentOperator) {
    // if the current pipeline has no operators we will remove it, because we want to omit empty pipelines
    if (!currentPipeline->hasOperators()) {
        auto successorPipeline = currentPipeline->getSuccessors()[0];
        pipelinePlan->removePipeline(currentPipeline);
        currentPipeline = successorPipeline;
    }
    // check if current operator is already part of a pipeline.
    // if yes lookup the pipeline fom the map. If not create a new one
    if (pipelineOperatorMap.find(currentOperator) != pipelineOperatorMap.end()) {
        pipelineOperatorMap[currentOperator]->addSuccessor(currentPipeline);
    } else {
        auto newPipeline = OperatorPipeline::create();
        pipelinePlan->addPipeline(newPipeline);
        newPipeline->addSuccessor(currentPipeline);
        process(pipelinePlan,
                pipelineOperatorMap,
                newPipeline,
                currentOperator->getChildren()[0]->as<PhysicalOperators::PhysicalOperator>());
        pipelineOperatorMap[currentOperator] = newPipeline;
    }
}

void DefaultPipeliningPhase::processPipelineBreakerOperator(const PipelineQueryPlanPtr& pipelinePlan,
                                                            std::map<OperatorNodePtr, OperatorPipelinePtr>& pipelineOperatorMap,
                                                            const OperatorPipelinePtr& currentPipeline,
                                                            const PhysicalOperators::PhysicalOperatorPtr& currentOperator) {
    // for pipeline breakers we create a new pipeline
    currentPipeline->prependOperator(currentOperator->as<PhysicalOperators::PhysicalOperator>()->copy());

    for (const auto& node : currentOperator->getChildren()) {
        auto newPipeline = OperatorPipeline::create();
        pipelinePlan->addPipeline(newPipeline);
        newPipeline->addSuccessor(currentPipeline);
        process(pipelinePlan, pipelineOperatorMap, newPipeline, node->as<PhysicalOperators::PhysicalOperator>());
    }
}

void DefaultPipeliningPhase::processFusibleOperator(const PipelineQueryPlanPtr& pipelinePlan,
                                                    std::map<OperatorNodePtr, OperatorPipelinePtr>& pipelineOperatorMap,
                                                    const OperatorPipelinePtr& currentPipeline,
                                                    const PhysicalOperators::PhysicalOperatorPtr& currentOperator) {
    // for operator we can fuse, we just append them to the current pipelie.
    currentPipeline->prependOperator(currentOperator->copy());
    for (const auto& node : currentOperator->getChildren()) {
        process(pipelinePlan, pipelineOperatorMap, currentPipeline, node->as<PhysicalOperators::PhysicalOperator>());
    }
}

void DefaultPipeliningPhase::processSink(const PipelineQueryPlanPtr& pipelinePlan,
                                         std::map<OperatorNodePtr, OperatorPipelinePtr>& pipelineOperatorMap,
                                         const OperatorPipelinePtr& currentPipeline,
                                         const PhysicalOperators::PhysicalOperatorPtr& currentOperator) {
    for (const auto& child : currentOperator->getChildren()) {
        auto cp = OperatorPipeline::create();
        pipelinePlan->addPipeline(cp);
        cp->addSuccessor(currentPipeline);
        process(pipelinePlan, pipelineOperatorMap, cp, child->as<PhysicalOperators::PhysicalOperator>());
    }
}

void DefaultPipeliningPhase::processSource(const PipelineQueryPlanPtr& pipelinePlan,
                                           std::map<OperatorNodePtr, OperatorPipelinePtr>&,
                                           OperatorPipelinePtr currentPipeline,
                                           const PhysicalOperators::PhysicalOperatorPtr& sourceOperator) {
    // Source operators will always be part of their own pipeline.
    if (currentPipeline->hasOperators()) {
        auto newPipeline = OperatorPipeline::create();
        pipelinePlan->addPipeline(newPipeline);
        newPipeline->addSuccessor(currentPipeline);
        currentPipeline = newPipeline;
    }
    currentPipeline->setType(OperatorPipeline::Type::SourcePipelineType);
    currentPipeline->prependOperator(sourceOperator->copy());
}

void DefaultPipeliningPhase::process(const PipelineQueryPlanPtr& pipelinePlan,
                                     std::map<OperatorNodePtr, OperatorPipelinePtr>& pipelineOperatorMap,
                                     const OperatorPipelinePtr& currentPipeline,
                                     const PhysicalOperators::PhysicalOperatorPtr& currentOperators) {

    // Pipelining only works on physical operators.
    if (!currentOperators->instanceOf<PhysicalOperators::PhysicalOperator>()) {
        throw QueryCompilationException("Pipelining can only be applied to physical operator. But current operator was: "
                                        + currentOperators->toString());
    }

    // Depending on the operator we apply different pipelining strategies
    if (currentOperators->instanceOf<PhysicalOperators::PhysicalSourceOperator>()) {
        processSource(pipelinePlan, pipelineOperatorMap, currentPipeline, currentOperators);
    } else if (currentOperators->instanceOf<PhysicalOperators::PhysicalSinkOperator>()) {
        processSink(pipelinePlan, pipelineOperatorMap, currentPipeline, currentOperators);
    } else if (currentOperators->instanceOf<PhysicalOperators::PhysicalUnionOperator>()) {
        processMultiplex(pipelinePlan, pipelineOperatorMap, currentPipeline, currentOperators);
    } else if (currentOperators->instanceOf<PhysicalOperators::PhysicalDemultiplexOperator>()) {
        processDemultiplex(pipelinePlan, pipelineOperatorMap, currentPipeline, currentOperators);
    } else if (operatorFusionPolicy->isFusible(currentOperators)) {
        processFusibleOperator(pipelinePlan, pipelineOperatorMap, currentPipeline, currentOperators);
    } else {
        processPipelineBreakerOperator(pipelinePlan, pipelineOperatorMap, currentPipeline, currentOperators);
    }
}
}// namespace NES::QueryCompilation