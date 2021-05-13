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

#include <NodeEngine/Execution/ExecutablePipeline.hpp>
#include <NodeEngine/Execution/ExecutablePipelineStage.hpp>
#include <NodeEngine/Execution/PipelineExecutionContext.hpp>
#include <NodeEngine/QueryManager.hpp>
#include <QueryCompiler/CCodeGenerator/CCodeGenerator.hpp>
#include <QueryCompiler/GeneratedQueryExecutionPlanBuilder.hpp>
#include <QueryCompiler/PipelineContext.hpp>
#include <QueryCompiler/QueryCompiler.hpp>
#include <Windowing/WindowAggregations/ExecutableSumAggregation.hpp>
#include <set>
#include <utility>
namespace NES {

QueryCompiler::QueryCompiler(uint64_t numberOfBuffersPerPipeline) : numberOfBuffersPerPipeline(numberOfBuffersPerPipeline) {
    // nop
}

QueryCompilerPtr QueryCompiler::create(uint64_t numberOfBuffersPerPipeline) {
    return std::make_shared<QueryCompiler>(numberOfBuffersPerPipeline);
}

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
    auto const qp = queryPlan->as<GeneratableOperator>();
    qp->produce(codeGenerator, context);
    compilePipelineStages(qepBuilder, codeGenerator, context);
}

namespace detail {

class PipelineStageHolder {
  public:
    uint32_t currentStageId;
    NodeEngine::Execution::ExecutablePipelineStagePtr executablePipelineStage;
    std::vector<NodeEngine::Execution::OperatorHandlerPtr> operatorHandlers;
    std::set<uint32_t> producers;
    std::set<uint32_t> consumers;
    SchemaPtr inputSchema;
    SchemaPtr outputSchema;

  public:
    PipelineStageHolder() = default;

    PipelineStageHolder(uint32_t currentStageId,
                        NodeEngine::Execution::ExecutablePipelineStagePtr executablePipelineStage,
                        const std::vector<NodeEngine::Execution::OperatorHandlerPtr> operatorHandlers,
                        SchemaPtr inputSchema,
                        SchemaPtr outputSchema)
        : currentStageId(currentStageId), executablePipelineStage(std::move(executablePipelineStage)),
          operatorHandlers(std::move(operatorHandlers)), inputSchema(inputSchema), outputSchema(outputSchema) {
        // nop
    }
};

void generateExecutablePipelines(QueryId queryId,
                                 QuerySubPlanId querySubPlanId,
                                 CodeGeneratorPtr codeGenerator,
                                 NodeEngine::BufferManagerPtr,
                                 NodeEngine::QueryManagerPtr,
                                 PipelineContextPtr context,
                                 std::map<uint32_t, PipelineStageHolder, std::greater<>>& accumulator) {
    // BFS visit to figure out producer-consumer relations among pipelines
    std::deque<std::tuple<int32_t, int32_t, PipelineContextPtr, int32_t>> queue;
    // we put the last pipeline here
    queue.emplace_back(0, -1, std::move(context), 0);
    uint32_t id = 1;

    while (!queue.empty()) {
        auto [currentPipelineStateId, consumerPipelineStateId, currContext, layer] = queue.front();
        queue.pop_front();
        try {
            NES_DEBUG("QueryCompiler: Layer: " << layer << " Compile query:" << queryId << " querySubPlan:" << querySubPlanId
                                               << " pipeline:" << currentPipelineStateId);
            // TODO we should set the proper pipeline name and ID during pipeline creation
            currContext->pipelineName = std::to_string(currentPipelineStateId);
            auto executablePipelineStage = codeGenerator->compile(currContext);
            if (executablePipelineStage == nullptr) {
                NES_ERROR("Cannot compile pipeline:" << currContext->code);
                NES_THROW_RUNTIME_ERROR("Cannot compile pipeline");
            }

            auto inputSchema = currContext->inputSchema;
            auto resultSchema = currContext->resultSchema;

            accumulator[currentPipelineStateId] = PipelineStageHolder(currentPipelineStateId,
                                                                      executablePipelineStage,
                                                                      currContext->getOperatorHandlers(),
                                                                      inputSchema,
                                                                      resultSchema);
            if (consumerPipelineStateId >= 0) {
                accumulator[currentPipelineStateId].consumers.emplace(consumerPipelineStateId);
            }

        } catch (std::exception& err) {
            NES_ERROR("Error while compiling pipeline: " << err.what());
            NES_THROW_RUNTIME_ERROR("Cannot compile pipeline");
        }

        for (const auto& nextPipelineContext : currContext->getNextPipelineContexts()) {
            queue.emplace_back(currentPipelineStateId + id, currentPipelineStateId, nextPipelineContext, layer + 1);
            accumulator[currentPipelineStateId].producers.emplace(currentPipelineStateId + id);
            id++;
        }
    }
}
}// namespace detail
void QueryCompiler::compilePipelineStages(GeneratedQueryExecutionPlanBuilder&, CodeGeneratorPtr, PipelineContextPtr) {
    /**
    std::map<uint32_t, detail::PipelineStageHolder, std::greater<>> executableStages;
    detail::generateExecutablePipelines(builder.getQueryId(), builder.getQuerySubPlanId(), std::move(codeGenerator),
                                        builder.getBufferManager(), builder.getQueryManager(), std::move(context),
                                        executableStages);
    if (executableStages.empty()) {
        NES_ERROR("compilePipelineStages failure: no pipelines to generate");
        NES_THROW_RUNTIME_ERROR("No pipelines generated");
    }
    auto queryManager = builder.getQueryManager();
    std::map<uint32_t, NodeEngine::Execution::ExecutablePipelinePtr> pipelines;
    for (auto it = executableStages.rbegin(), last = executableStages.rend(); it != last; ++it) {
        auto& [stageId, holder] = *it;
        NodeEngine::Execution::PipelineExecutionContextPtr executionContext;
        if (!holder.consumers.empty()) {
            // invoke next pipeline
            std::vector<NodeEngine::Execution::ExecutablePipelinePtr> childPipelines;
            for (auto childStageId : holder.consumers) {
                childPipelines.emplace_back(pipelines[childStageId]);
            }
            executionContext = std::make_shared<NodeEngine::Execution::PipelineExecutionContext>(
                builder.getQuerySubPlanId(), builder.getQueryManager(), builder.getBufferManager(),
                [childPipelines](NodeEngine::TupleBuffer& buffer, NodeEngine::WorkerContextRef workerContext) {
                    for (auto& childPipeline : childPipelines) {
                        childPipeline->execute(buffer, workerContext);
                    }
                },
                [childPipelines, queryManager](NodeEngine::TupleBuffer& buffer) {
                    for (auto& childPipeline : childPipelines) {
                        queryManager->addWorkForNextPipeline(buffer, childPipeline);
                    }
                },
                holder.operatorHandlers, numberOfBuffersPerPipeline);
        } else {
            // invoke sink
            auto& sinks = builder.getSinks();
            if (sinks.empty()) {
                NES_ERROR("compilePipelineStages failure: no sinks for " << builder.getQueryId());
                NES_THROW_RUNTIME_ERROR("No sinks available in query plan");
            }
            executionContext = std::make_shared<NodeEngine::Execution::PipelineExecutionContext>(
                builder.getQuerySubPlanId(), builder.getQueryManager(), builder.getBufferManager(),
                [sinks](NodeEngine::TupleBuffer& buffer, NodeEngine::WorkerContextRef workerContext) {
                    for (auto& sink : sinks) {
                        sink->writeData(buffer, workerContext);
                    }
                },
                [sinks, builder](NodeEngine::TupleBuffer&) {
                    NES_ERROR("QueryCompiler: we cant emit to a sink, if no worker context is provided");
                },
                holder.operatorHandlers, numberOfBuffersPerPipeline);
        }
        uint32_t numOfProducers = holder.producers.size();
        //This is not something to look at, please pass by
        if (numOfProducers == 0) {
            for (const auto& source : builder.getSources()) {
                SchemaPtr sourceSchema = source->getSchema();
                if (holder.inputSchema && holder.inputSchema->equals(sourceSchema)) {
                    ++numOfProducers;
                }
            }
        }

        auto pipeline = NodeEngine::Execution::ExecutablePipeline::create(
            stageId, builder.getQuerySubPlanId(), holder.executablePipelineStage, executionContext, numOfProducers,
            pipelines[*holder.consumers.begin()], holder.inputSchema, holder.outputSchema);

        NES_DEBUG("pipeline code for id=" << pipeline->getPipeStageId() << "=" << pipeline->getCodeAsString());
        builder.addPipeline(pipeline);
        pipelines[stageId] = pipeline;
    }
    **/
}

QueryCompilerPtr createDefaultQueryCompiler(uint64_t numberOfBuffersPerPipeline) {
    return QueryCompiler::create(numberOfBuffersPerPipeline);
}

}// namespace NES