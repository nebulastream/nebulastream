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

#include <NodeEngine/QueryManager.hpp>
#include <QueryCompiler/CCodeGenerator/CCodeGenerator.hpp>
#include <QueryCompiler/GeneratedQueryExecutionPlan.hpp>
#include <QueryCompiler/GeneratedQueryExecutionPlanBuilder.hpp>
#include <QueryCompiler/PipelineContext.hpp>
#include <QueryCompiler/PipelineExecutionContext.hpp>
#include <QueryCompiler/QueryCompiler.hpp>
#include <Windowing/WindowAggregations/ExecutableSumAggregation.hpp>
#include <set>
#include <utility>
namespace NES {

QueryCompiler::QueryCompiler() {
    // nop
}

QueryCompilerPtr QueryCompiler::create() { return std::make_shared<QueryCompiler>(); }

// TODO compiler folks please check the following statements
/**
 * The following code builds a dataflow graph of pipeline stages. It does so by following these steps:
 * 1. generates executable pipelines for an input query plan that is represented as a tree (should be graph) whose root (should be sink nodes) is a sink operator
 * 2. performs a BFS visit of the tree to figure out producer-consumer relations among pipeline stages (this is necessary because we need to label operators)
 * 3. while BFS-visiting the tree, it inserts the pipelines in a sorted map (leaves stages are the first)
 * 4. it scans the map to build pipeline stages. This way, we know the consumer set for each pipeline stage (or its sinks) and we can generate buffer emitters
 */

void QueryCompiler::compile(GeneratedQueryExecutionPlanBuilder& qepBuilder, OperatorNodePtr queryPlan) {
    auto codeGenerator = CCodeGenerator::create();
    auto context = PipelineContext::create();
    queryPlan->as<GeneratableOperator>()->produce(codeGenerator, context);
    compilePipelineStages(qepBuilder, codeGenerator, context);
}

namespace detail {

class PipelineStageHolder {
  public:
    uint32_t currentStageId;
    ExecutablePipelinePtr executablePipeline;
    Windowing::AbstractWindowHandlerPtr windowHandler;
    Join::AbstractJoinHandlerPtr joinHandler;
    std::set<uint32_t> producers;
    std::set<uint32_t> consumers;

  public:
    PipelineStageHolder() = default;

    PipelineStageHolder(uint32_t currentStageId, ExecutablePipelinePtr executablePipeline,
                        Windowing::AbstractWindowHandlerPtr windowHandler, Join::AbstractJoinHandlerPtr joinHandler)
        : currentStageId(currentStageId), executablePipeline(std::move(executablePipeline)),
          windowHandler(std::move(windowHandler)), joinHandler(std::move(joinHandler)) {
        // nop
    }
};

void generateExecutablePipelines(QueryId queryId, QuerySubPlanId querySubPlanId, CodeGeneratorPtr codeGenerator, BufferManagerPtr,
                                 QueryManagerPtr, PipelineContextPtr context,
                                 std::map<uint32_t, PipelineStageHolder, std::greater<>>& accumulator) {
    // BFS visit to figure out producer-consumer relations among pipelines
    std::deque<std::tuple<int32_t, int32_t, PipelineContextPtr>> queue;
    queue.emplace_back(0, -1, std::move(context));
    while (!queue.empty()) {
        auto [currentPipelineStateId, consumerPipelineStateId, currContext] = queue.front();
        queue.pop_front();
        try {
            NES_DEBUG("QueryCompiler: Compile query:" << queryId << " querySubPlan:" << querySubPlanId
                                                      << " pipeline:" << currentPipelineStateId);
            auto executablePipeline = codeGenerator->compile(currContext->code);
            if (executablePipeline == nullptr) {
                NES_ERROR("Cannot compile pipeline:" << currContext->code);
                NES_THROW_RUNTIME_ERROR("Cannot compile pipeline");
            }
            Windowing::AbstractWindowHandlerPtr windowHandlerPtr = nullptr;
            Join::AbstractJoinHandlerPtr joinHandlerPtr = nullptr;
            if (currContext->hasWindow()) {
                NES_DEBUG("generateExecutablePipelines add window handler");
                windowHandlerPtr = currContext->getWindow();
            }
            if (currContext->hasJoin()) {
                NES_DEBUG("generateExecutablePipelines add join handler");
                joinHandlerPtr = currContext->getJoin();
            }

            accumulator[currentPipelineStateId] =
                PipelineStageHolder(currentPipelineStateId, executablePipeline, windowHandlerPtr, joinHandlerPtr);
            if (consumerPipelineStateId >= 0) {
                accumulator[currentPipelineStateId].consumers.emplace(consumerPipelineStateId);
            }

        } catch (std::exception& err) {
            NES_ERROR("Error while compiling pipeline: " << err.what());
            NES_THROW_RUNTIME_ERROR("Cannot compile pipeline");
        }
        uint32_t i = 1;
        for (const auto& nextPipelineContext : currContext->getNextPipelineContexts()) {
            queue.emplace_back(currentPipelineStateId + i, currentPipelineStateId, nextPipelineContext);
            accumulator[currentPipelineStateId].producers.emplace(currentPipelineStateId + i);
            i++;
        }
    }
}
}// namespace detail
void QueryCompiler::compilePipelineStages(GeneratedQueryExecutionPlanBuilder& builder, CodeGeneratorPtr codeGenerator,
                                          PipelineContextPtr context) {

    std::map<uint32_t, detail::PipelineStageHolder, std::greater<>> executableStages;
    detail::generateExecutablePipelines(builder.getQueryId(), builder.getQuerySubPlanId(), std::move(codeGenerator),
                                        builder.getBufferManager(), builder.getQueryManager(), std::move(context),
                                        executableStages);

    if (executableStages.empty()) {
        NES_ERROR("compilePipelineStages failure: no pipelines to generate");
        NES_THROW_RUNTIME_ERROR("No pipelines generated");
    }

    std::map<uint32_t, PipelineStagePtr> pipelines;
    for (auto it = executableStages.rbegin(), last = executableStages.rend(); it != last; ++it) {
        auto& [stageId, holder] = *it;
        QueryExecutionContextPtr executionContext;
        if (!holder.consumers.empty()) {
            // invoke next pipeline
            std::vector<PipelineStagePtr> childPipelines;
            for (auto childStageId : holder.consumers) {
                childPipelines.emplace_back(pipelines[childStageId]);
            }
            executionContext = std::make_shared<PipelineExecutionContext>(
                builder.getQuerySubPlanId(), builder.getBufferManager(),
                [childPipelines](TupleBuffer& buffer, WorkerContextRef workerContext) {
                    for (auto& childPipeline : childPipelines) {
                        childPipeline->execute(buffer, workerContext);
                    }
                },
                holder.windowHandler, holder.joinHandler);
        } else {
            // invoke sink
            auto& sinks = builder.getSinks();
            if (sinks.empty()) {
                NES_ERROR("compilePipelineStages failure: no sinks for " << builder.getQueryId());
                NES_THROW_RUNTIME_ERROR("No sinks available in query plan");
            }
            executionContext = std::make_shared<PipelineExecutionContext>(
                builder.getQuerySubPlanId(), builder.getBufferManager(),
                [sinks](TupleBuffer& buffer, WorkerContextRef workerContext) {
                    for (auto& sink : sinks) {
                        sink->writeData(buffer, workerContext);
                    }
                },
                holder.windowHandler, holder.joinHandler);
        }
        PipelineStagePtr pipelineStage = PipelineStage::create(stageId, builder.getQuerySubPlanId(), holder.executablePipeline,
                                                               executionContext, pipelines[*holder.consumers.begin()]);

        builder.addPipelineStage(pipelineStage);
        pipelines[stageId] = pipelineStage;
    }
}

QueryCompilerPtr createDefaultQueryCompiler() { return QueryCompiler::create(); }

}// namespace NES