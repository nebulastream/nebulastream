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

#include <Configurations/Enums/QueryCompilerType.hpp>
#include <Configurations/Enums/WindowingStrategy.hpp>
#include <ExportPhaseFactory.h>
#include <OperatorHandlerTracer.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <QueryCompiler/Operators/NautilusPipelineOperator.hpp>
#include <QueryCompiler/Operators/PipelineQueryPlan.hpp>
#include <QueryCompiler/Phases/AddScanAndEmitPhase.hpp>
#include <QueryCompiler/Phases/BufferOptimizationPhase.hpp>
#include <QueryCompiler/Phases/Pipelining/PipeliningPhase.hpp>
#include <QueryCompiler/Phases/Translations/LowerLogicalToPhysicalOperators.hpp>
#include <QueryCompiler/Phases/Translations/LowerPhysicalToNautilusOperators.hpp>
#include <QueryCompiler/QueryCompilerOptions.hpp>
#include <Stage/QueryPipeliner.hpp>
namespace NES::Unikernel::Export {
Stage Stage::from(
    IntermediateStage&& im,
    const std::unordered_map<Runtime::Unikernel::GlobalOperatorHandlerIndex, Runtime::Execution::OperatorHandlerPtr>& handlerById,
    const std::unordered_map<Runtime::Unikernel::GlobalOperatorHandlerIndex, size_t>& uniqueOperatorHandlers,
    const std::unordered_map<Runtime::Unikernel::GlobalOperatorHandlerIndex, SharedOperatorHandlerIndex>& sharedByGlobal) {

    std::vector<std::pair<LocalOperatorHandlerIndex, Runtime::Unikernel::OperatorHandlerDescriptor>> descriptors;
    std::vector<std::pair<LocalOperatorHandlerIndex, Runtime::Unikernel::GlobalOperatorHandlerIndex>> sharedHandlersIndices;
    LocalOperatorHandlerIndex currentLocalIndex = 0;

    for (const auto& operatorHandler : im.operatorHandlers) {
        if (uniqueOperatorHandlers.at(operatorHandler) == 1) {
            descriptors.emplace_back(currentLocalIndex, handlerById.at(operatorHandler)->getDescriptor());
        } else {
            sharedHandlersIndices.emplace_back(currentLocalIndex, sharedByGlobal.at(operatorHandler));
        }
        currentLocalIndex++;
    }
    return {std::move(descriptors),
            std::move(sharedHandlersIndices),
            std::move(im.pipeline),
            im.successors,
            std::move(im.predecessor)};
}
QueryCompilation::QueryCompilerOptionsPtr QueryPipeliner::createOptions() {
    QueryCompilation::QueryCompilerOptionsPtr queryCompilerOptions =
        QueryCompilation::QueryCompilerOptions::createDefaultOptions();
    queryCompilerOptions->setQueryCompiler(QueryCompilation::QueryCompilerType::NAUTILUS_QUERY_COMPILER);
    queryCompilerOptions->setWindowingStrategy(QueryCompilation::WindowingStrategy::SLICING);
    return queryCompilerOptions;
}
IntermediateStage QueryPipeliner::lowerToNautilus(const QueryCompilation::OperatorPipelinePtr& pipeline) {
    const auto lowerToNautilus = std::make_shared<QueryCompilation::LowerPhysicalToNautilusOperators>(options);
    std::optional<PipelineId> successor = std::nullopt;
    if (!pipeline->getSuccessors().empty()) {
        successor.emplace(pipeline->getSuccessors()[0]->getPipelineId());
    }

    std::vector<PipelineId> predecessor;
    std::ranges::transform(pipeline->getPredecessors(), std::back_inserter(predecessor), [](auto pred) {
        return pred->getPipelineId();
    });

    if (!pipeline->isOperatorPipeline()) {
        return IntermediateStage{{}, pipeline, successor, std::move(predecessor)};
    }

    const auto nautilusPipeline = lowerToNautilus->apply(pipeline, bufferSize);
    std::vector<Runtime::Unikernel::GlobalOperatorHandlerIndex> handlerIds;
    auto handlers = nautilusPipeline->getQueryPlan()
                        ->getRootOperators()[0]
                        ->as<QueryCompilation::NautilusPipelineOperator>()
                        ->getOperatorHandlers();
    if (!handlers.empty()) {
        std::stringstream ss;
        for (const auto& handler : handlers) {
            ss << "\t" << handler->getDescriptor().className << "(" << handler->getDescriptor().handlerId << ")\n";
        }
        NES_INFO("OperatorHandler Tracer: Stage {} uses:\n{}", pipeline->getPipelineId(), ss.str());
    }
    std::ranges::for_each(handlers, [this, &handlerIds](const auto& handler) {
        handlerIndexCounter[handler->getDescriptor().handlerId] += 1;
        handlerByHandlerIndex[handler->getDescriptor().handlerId] = handler;
        handlerIds.emplace_back(handler->getDescriptor().handlerId);
    });

    return IntermediateStage{handlerIds, nautilusPipeline, successor, std::move(predecessor)};
}
QueryPipeliner::Result QueryPipeliner::lowerQuery(const QueryPlanPtr& unikernelWorkerQueryPlan) {
    const auto phaseFactory = QueryCompilation::Phases::ExportPhaseFactory::create();

    Runtime::Unikernel::OperatorHandlerTracer::reset();
    NES_INFO("Initial Unikernel Query Plan:\n{}", unikernelWorkerQueryPlan->toString());
    const auto logicalPlan = phaseFactory->createLowerLogicalQueryPlanPhase(options)->apply(unikernelWorkerQueryPlan);
    NES_INFO("Logical Unikernel Query Plan:\n{}", logicalPlan->toString());
    const auto initialPipelinedPlan = phaseFactory->createPipeliningPhase(options)->apply(logicalPlan);
    NES_INFO("Pipelined Unikernel Query Plan:\n{}", initialPipelinedPlan->toString());
    const auto pipelinedPlanBufferOptimized = phaseFactory->createBufferOptimizationPhase(options)->apply(initialPipelinedPlan);
    NES_INFO("Applied Buffer Optimzations");
    const auto pipelinedPlan = phaseFactory->createAddScanAndEmitPhase(options)->apply(pipelinedPlanBufferOptimized);

    std::vector<IntermediateStage> intermediateStages;
    std::ranges::transform(pipelinedPlan->getPipelines(), std::back_inserter(intermediateStages), [this](auto&& pipeline) {
        return lowerToNautilus(pipeline);
    });

    auto [sharedHandler, sharedByGlobal] = sharedHandlers();

    std::vector<Stage> stages;
    std::ranges::transform(
        std::move(intermediateStages),
        std::back_inserter(stages),
        [this, &sharedByGlobal](IntermediateStage& intermediateStage) {
            return Stage::from(std::move(intermediateStage), handlerByHandlerIndex, handlerIndexCounter, sharedByGlobal);
        });

    return {std::move(sharedHandler), std::move(stages)};
}

std::pair<std::unordered_map<SharedOperatorHandlerIndex, Runtime::Unikernel::OperatorHandlerDescriptor>,
          std::unordered_map<Runtime::Unikernel::GlobalOperatorHandlerIndex, SharedOperatorHandlerIndex>>
QueryPipeliner::sharedHandlers() {
    std::unordered_map<SharedOperatorHandlerIndex, Runtime::Unikernel::OperatorHandlerDescriptor> sharedHandlers;
    std::unordered_map<Runtime::Unikernel::GlobalOperatorHandlerIndex, SharedOperatorHandlerIndex> sharedByGlobal;
    SharedOperatorHandlerIndex currentSharedHandlerIndex = 0;

    for (const auto& [globalIndex, users] : handlerIndexCounter) {
        if (users > 1) {
            NES_INFO("OperatorHandler Tracer: Shared Handler {}",
                     handlerByHandlerIndex.at(globalIndex)->getDescriptor().className);
            sharedHandlers[currentSharedHandlerIndex] = handlerByHandlerIndex.at(globalIndex)->getDescriptor();
            sharedByGlobal[globalIndex] = currentSharedHandlerIndex;
            currentSharedHandlerIndex++;
        }
    }

    return {sharedHandlers, sharedByGlobal};
}
}// namespace NES::Unikernel::Export