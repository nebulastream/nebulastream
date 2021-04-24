/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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
#include <Operators/OperatorNode.hpp>
#include <Plans/Utils/QueryPlanIterator.hpp>
#include <QueryCompiler/Operators/OperatorPipeline.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalDemultiplexOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalMultiplexOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalSinkOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalSourceOperator.hpp>
#include <QueryCompiler/Operators/PipelineQueryPlan.hpp>
#include <QueryCompiler/Phases/Pipelining/DefaultPipeliningPhase.hpp>
#include <QueryCompiler/Phases/Pipelining/PipelineBreakerPolicy.hpp>

namespace NES {
namespace QueryCompilation {

DefaultPipeliningPhase::DefaultPipeliningPhase(PipelineBreakerPolicyPtr pipelineBreakerPolicy)
    : pipelineBreakerPolicy(pipelineBreakerPolicy) {}

PipeliningPhasePtr DefaultPipeliningPhase::create(PipelineBreakerPolicyPtr pipelineBreakerPolicy) {
    return std::make_shared<DefaultPipeliningPhase>(pipelineBreakerPolicy);
}

void DefaultPipeliningPhase::processMultiplex(PipelineQueryPlanPtr pipelinePlan,
                                              std::map<OperatorNodePtr, OperatorPipelinePtr>& pipelineOperatorMap,
                                              OperatorPipelinePtr currentPipeline,
                                              PhysicalOperators::PhysicalOperatorPtr currentOperator) {
    if (!currentPipeline->hasOperators()) {
        auto successorPipeline = currentPipeline->getSuccessors()[0];
        pipelinePlan->removePipeline(currentPipeline);
        currentPipeline = successorPipeline;
    }
    for (auto node : currentOperator->getChildren()) {
        auto newPipeline = OperatorPipeline::create();
        pipelinePlan->addPipeline(newPipeline);
        newPipeline->addSuccessor(currentPipeline);
        process(pipelinePlan, pipelineOperatorMap, newPipeline, node->as<PhysicalOperators::PhysicalOperator>());
    }
}

void DefaultPipeliningPhase::processDemultiplex(PipelineQueryPlanPtr pipelinePlan,
                                                std::map<OperatorNodePtr, OperatorPipelinePtr>& pipelineOperatorMap,
                                                OperatorPipelinePtr currentPipeline,
                                                PhysicalOperators::PhysicalOperatorPtr currentOperator) {
    if (!currentPipeline->hasOperators()) {
        auto successorPipeline = currentPipeline->getSuccessors()[0];
        pipelinePlan->removePipeline(currentPipeline);
        currentPipeline = successorPipeline;
    }
    if (pipelineOperatorMap.find(currentOperator) != pipelineOperatorMap.end()) {
        pipelineOperatorMap[currentOperator]->addSuccessor(currentPipeline);
    } else {
        auto newPipeline = OperatorPipeline::create();
        pipelinePlan->addPipeline(newPipeline);
        newPipeline->addSuccessor(currentPipeline);
        process(pipelinePlan, pipelineOperatorMap, newPipeline, currentOperator->getChildren()[0]->as<PhysicalOperators::PhysicalOperator>());
        pipelineOperatorMap[currentOperator] = newPipeline;
    }
}

void DefaultPipeliningPhase::processPipelineBreakerOperator(
    PipelineQueryPlanPtr pipelinePlan, std::map<OperatorNodePtr, OperatorPipelinePtr>& pipelineOperatorMap,
                                                            OperatorPipelinePtr currentPipeline, PhysicalOperators::PhysicalOperatorPtr currentOperator) {
    currentPipeline->prependOperator(currentOperator->as<PhysicalOperators::PhysicalOperator>()->copy());
    for (auto node : currentOperator->getChildren()) {
        auto newPipeline = OperatorPipeline::create();
        pipelinePlan->addPipeline(newPipeline);
        newPipeline->addSuccessor(currentPipeline);
        process(pipelinePlan, pipelineOperatorMap, newPipeline, node->as<PhysicalOperators::PhysicalOperator>());
    }
}

void DefaultPipeliningPhase::processFusibleOperator(PipelineQueryPlanPtr pipelinePlan,
                                                    std::map<OperatorNodePtr, OperatorPipelinePtr>& pipelineOperatorMap,
                                                    OperatorPipelinePtr currentPipeline,
                                                    PhysicalOperators::PhysicalOperatorPtr currentOperator) {
    currentPipeline->prependOperator(currentOperator->copy());
    for (auto node : currentOperator->getChildren()) {
        process(pipelinePlan, pipelineOperatorMap, currentPipeline, node->as<PhysicalOperators::PhysicalOperator>());
    }
}

void DefaultPipeliningPhase::processSink(PipelineQueryPlanPtr pipelinePlan,
                                         std::map<OperatorNodePtr, OperatorPipelinePtr>& pipelineOperatorMap,
                                         OperatorPipelinePtr currentPipeline,
                                         PhysicalOperators::PhysicalOperatorPtr currentOperator) {
    for (auto child : currentOperator->getChildren()) {
        auto cp = OperatorPipeline::create();
        pipelinePlan->addPipeline(cp);
        cp->addSuccessor(currentPipeline);
        process(pipelinePlan, pipelineOperatorMap, cp, child->as<PhysicalOperators::PhysicalOperator>());
    }
}

void DefaultPipeliningPhase::processSource(PipelineQueryPlanPtr pipeline,
                                           std::map<OperatorNodePtr, OperatorPipelinePtr>&,
                                           OperatorPipelinePtr currentPipeline,
                                           PhysicalOperators::PhysicalOperatorPtr sourceOperator) {
    if (currentPipeline->hasOperators()) {
        auto newPipeline = OperatorPipeline::create();
        pipeline->addPipeline(newPipeline);
        newPipeline->addSuccessor(currentPipeline);
        currentPipeline = newPipeline;
    }
    currentPipeline->setType(OperatorPipeline::SourcePipelineType);
    currentPipeline->prependOperator(sourceOperator->copy());
}

void DefaultPipeliningPhase::process(PipelineQueryPlanPtr pipeline,
                                     std::map<OperatorNodePtr, OperatorPipelinePtr>& pipelineOperatorMap,
                                     OperatorPipelinePtr currentPipeline,
                                     PhysicalOperators::PhysicalOperatorPtr currentOperators) {
    if (currentOperators->instanceOf<PhysicalOperators::PhysicalSourceOperator>()) {
        processSource(pipeline, pipelineOperatorMap, currentPipeline, currentOperators);
    } else if (currentOperators->instanceOf<PhysicalOperators::PhysicalSinkOperator>()) {
        processSink(pipeline, pipelineOperatorMap, currentPipeline, currentOperators);
    } else if (currentOperators->instanceOf<PhysicalOperators::PhysicalMultiplexOperator>()) {
        processMultiplex(pipeline, pipelineOperatorMap, currentPipeline, currentOperators);
    } else if (currentOperators->instanceOf<PhysicalOperators::PhysicalDemultiplexOperator>()) {
        processDemultiplex(pipeline, pipelineOperatorMap, currentPipeline, currentOperators);
    } else if (pipelineBreakerPolicy->isFusible(currentOperators)) {
        processFusibleOperator(pipeline, pipelineOperatorMap, currentPipeline, currentOperators);
    } else {
        processPipelineBreakerOperator(pipeline, pipelineOperatorMap, currentPipeline, currentOperators);
    }
}

PipelineQueryPlanPtr DefaultPipeliningPhase::apply(QueryPlanPtr queryPlan) {
    std::map<OperatorNodePtr, OperatorPipelinePtr> pipelineOperatorMap;
    auto pipelinePlan = PipelineQueryPlan::create();
    for (auto sourceOperators : queryPlan->getRootOperators()) {
        auto pipeline = OperatorPipeline::createSinkPipeline();
        pipeline->prependOperator(sourceOperators->copy());
        pipelinePlan->addPipeline(pipeline);
        processSink(pipelinePlan, pipelineOperatorMap, pipeline, sourceOperators->as<PhysicalOperators::PhysicalOperator>());
    }
    return pipelinePlan;
}

}// namespace QueryCompilation
}// namespace NES