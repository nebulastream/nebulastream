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
#include <Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/LogicalStreamSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceDescriptor.hpp>
#include <Phases/ConvertLogicalToPhysicalSink.hpp>
#include <Phases/ConvertLogicalToPhysicalSource.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Plans/Utils/QueryPlanIterator.hpp>
#include <QueryCompiler/Exceptions/QueryCompilationException.hpp>
#include <QueryCompiler/Operators/ExecutableOperator.hpp>
#include <QueryCompiler/Operators/OperatorPipeline.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalSinkOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalSourceOperator.hpp>
#include <QueryCompiler/Operators/PipelineQueryPlan.hpp>
#include <QueryCompiler/Operators/PipelineQueryPlanIterator.hpp>
#include <QueryCompiler/Phases/Translations/DataSinkProvider.hpp>
#include <QueryCompiler/Phases/Translations/DataSourceProvider.hpp>
#include <QueryCompiler/Phases/Translations/GeneratableOperatorProvider.hpp>
#include <QueryCompiler/Phases/Translations/LowerToExecutableQueryPlanPhase.hpp>
#include <QueryCompiler/QueryCompilerForwardDeclaration.hpp>
#include <Runtime/Execution/ExecutablePipeline.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/NodeEngine.hpp>
#include <utility>
#include <variant>

namespace NES::QueryCompilation {
LowerToExecutableQueryPlanPhase::LowerToExecutableQueryPlanPhase(DataSinkProviderPtr sinkProvider,
                                                                 DataSourceProviderPtr sourceProvider)
    : sinkProvider(std::move(sinkProvider)), sourceProvider(std::move(sourceProvider)){};

LowerToExecutableQueryPlanPhasePtr LowerToExecutableQueryPlanPhase::create(const DataSinkProviderPtr& sinkProvider,
                                                                           const DataSourceProviderPtr& sourceProvider) {
    return std::make_shared<LowerToExecutableQueryPlanPhase>(sinkProvider, sourceProvider);
}

Runtime::Execution::ExecutableQueryPlanPtr LowerToExecutableQueryPlanPhase::apply(const PipelineQueryPlanPtr& pipelineQueryPlan,
                                                                                  const Runtime::NodeEnginePtr& nodeEngine) {
    std::vector<DataSourcePtr> sources;
    std::vector<DataSinkPtr> sinks;
    std::vector<Runtime::Execution::ExecutablePipelinePtr> executablePipelines;
    std::map<uint64_t, Runtime::Execution::SuccessorExecutablePipeline> pipelineToExecutableMap;
    //Process all pipelines recursively.
    auto sourcePipelines = pipelineQueryPlan->getSourcePipelines();
    for (const auto& pipeline : sourcePipelines) {
        processSource(pipeline,
                      sources,
                      sinks,
                      executablePipelines,
                      nodeEngine,
                      pipelineQueryPlan->getQueryId(),
                      pipelineQueryPlan->getQuerySubPlanId(),
                      pipelineToExecutableMap);
    }

    return std::make_shared<Runtime::Execution::ExecutableQueryPlan>(pipelineQueryPlan->getQueryId(),
                                                                     pipelineQueryPlan->getQuerySubPlanId(),
                                                                     std::move(sources),
                                                                     std::move(sinks),
                                                                     std::move(executablePipelines),
                                                                     nodeEngine->getQueryManager(),
                                                                     nodeEngine->getBufferManager());
}
Runtime::Execution::SuccessorExecutablePipeline LowerToExecutableQueryPlanPhase::processSuccessor(
    const OperatorPipelinePtr& pipeline,
    std::vector<DataSourcePtr>& sources,
    std::vector<DataSinkPtr>& sinks,
    std::vector<Runtime::Execution::ExecutablePipelinePtr>& executablePipelines,
    const Runtime::NodeEnginePtr& nodeEngine,
    QueryId queryId,
    QuerySubPlanId subQueryPlanId,
    std::map<uint64_t, Runtime::Execution::SuccessorExecutablePipeline>& pipelineToExecutableMap) {

    // check if the particular pipeline already exist in the pipeline map.
    if (pipelineToExecutableMap.find(pipeline->getPipelineId()) != pipelineToExecutableMap.end()) {
        return pipelineToExecutableMap[pipeline->getPipelineId()];
    }

    if (pipeline->isSinkPipeline()) {
        auto executableSink = processSink(pipeline, sources, sinks, executablePipelines, nodeEngine, queryId, subQueryPlanId);
        pipelineToExecutableMap[pipeline->getPipelineId()] = executableSink;
        return executableSink;
    }
    if (pipeline->isOperatorPipeline()) {
        auto executablePipeline = processOperatorPipeline(pipeline,
                                                          sources,
                                                          sinks,
                                                          executablePipelines,
                                                          nodeEngine,
                                                          queryId,
                                                          subQueryPlanId,
                                                          pipelineToExecutableMap);
        pipelineToExecutableMap[pipeline->getPipelineId()] = executablePipeline;
        return executablePipeline;
    }
    throw QueryCompilationException("The pipeline was of wrong type. It should be a sink pipeline or a operator pipeline");
}

void LowerToExecutableQueryPlanPhase::processSource(
    const OperatorPipelinePtr& pipeline,
    std::vector<DataSourcePtr>& sources,
    std::vector<DataSinkPtr>& sinks,
    std::vector<Runtime::Execution::ExecutablePipelinePtr>& executablePipelines,
    const Runtime::NodeEnginePtr& nodeEngine,
    QueryId queryId,
    QuerySubPlanId subQueryPlanId,
    std::map<uint64_t, Runtime::Execution::SuccessorExecutablePipeline>& pipelineToExecutableMap) {

    if (!pipeline->isSourcePipeline()) {
        NES_ERROR("This is not a source pipeline.");
        NES_ERROR(pipeline->getQueryPlan()->toString());
        throw QueryCompilationException("This is not a source pipeline.");
    }

    auto rootOperator = pipeline->getQueryPlan()->getRootOperators()[0];
    auto sourceOperator = rootOperator->as<PhysicalOperators::PhysicalSourceOperator>();
    auto sourceDescriptor = sourceOperator->getSourceDescriptor();
    if (sourceDescriptor->instanceOf<LogicalStreamSourceDescriptor>()) {
        sourceDescriptor = nodeEngine->createLogicalSourceDescriptor(sourceDescriptor);
    }

    std::vector<Runtime::Execution::SuccessorExecutablePipeline> executableSuccessorPipelines;
    for (const auto& successor : pipeline->getSuccessors()) {
        auto executableSuccessor = processSuccessor(successor,
                                                    sources,
                                                    sinks,
                                                    executablePipelines,
                                                    nodeEngine,
                                                    queryId,
                                                    subQueryPlanId,
                                                    pipelineToExecutableMap);
        executableSuccessorPipelines.emplace_back(executableSuccessor);
    }

    auto source = sourceProvider->lower(sourceOperator->getId(),
                                        sourceOperator->getLogicalSourceOperatorId(),
                                        sourceDescriptor,
                                        nodeEngine,
                                        executableSuccessorPipelines);
    sources.emplace_back(source);
}

Runtime::Execution::SuccessorExecutablePipeline
LowerToExecutableQueryPlanPhase::processSink(const OperatorPipelinePtr& pipeline,
                                             std::vector<DataSourcePtr>&,
                                             std::vector<DataSinkPtr>& sinks,
                                             std::vector<Runtime::Execution::ExecutablePipelinePtr>&,
                                             Runtime::NodeEnginePtr nodeEngine,
                                             QueryId,
                                             QuerySubPlanId subQueryPlanId) {
    auto rootOperator = pipeline->getQueryPlan()->getRootOperators()[0];
    auto sinkOperator = rootOperator->as<PhysicalOperators::PhysicalSinkOperator>();
    auto sink = sinkProvider->lower(sinkOperator->getId(),
                                    sinkOperator->getLogicalOperatorId(),
                                    sinkOperator->getSinkDescriptor(),
                                    sinkOperator->getOutputSchema(),
                                    std::move(nodeEngine),
                                    subQueryPlanId);
    sinks.emplace_back(sink);
    return sink;
}

Runtime::Execution::SuccessorExecutablePipeline LowerToExecutableQueryPlanPhase::processOperatorPipeline(
    const OperatorPipelinePtr& pipeline,
    std::vector<DataSourcePtr>& sources,
    std::vector<DataSinkPtr>& sinks,
    std::vector<Runtime::Execution::ExecutablePipelinePtr>& executablePipelines,
    const Runtime::NodeEnginePtr& nodeEngine,
    QueryId queryId,
    QuerySubPlanId subQueryPlanId,
    std::map<uint64_t, Runtime::Execution::SuccessorExecutablePipeline>& pipelineToExecutableMap) {

    auto rootOperator = pipeline->getQueryPlan()->getRootOperators()[0];
    auto executableOperator = rootOperator->as<ExecutableOperator>();

    std::vector<Runtime::Execution::SuccessorExecutablePipeline> executableSuccessorPipelines;
    for (const auto& successor : pipeline->getSuccessors()) {
        auto executableSuccessor = processSuccessor(successor,
                                                    sources,
                                                    sinks,
                                                    executablePipelines,
                                                    nodeEngine,
                                                    queryId,
                                                    subQueryPlanId,
                                                    pipelineToExecutableMap);
        executableSuccessorPipelines.emplace_back(executableSuccessor);
    }

    auto emitToSuccessorFunctionHandler = [executableSuccessorPipelines](Runtime::TupleBuffer& buffer,
                                                                         Runtime::WorkerContextRef workerContext) {
        for (const auto& executableSuccessor : executableSuccessorPipelines) {
            if (const auto* sink = std::get_if<DataSinkPtr>(&executableSuccessor)) {
                NES_DEBUG("Emit Buffer to data sink" << (*sink)->toString());
                (*sink)->writeData(buffer, workerContext);
            } else if (const auto* nextExecutablePipeline =
                           std::get_if<Runtime::Execution::ExecutablePipelinePtr>(&executableSuccessor)) {
                NES_DEBUG("Emit Buffer to pipeline" << (*nextExecutablePipeline)->getPipelineId());
                (*nextExecutablePipeline)->execute(buffer, workerContext);
            }
        }
    };

    auto emitToQueryManagerFunctionHandler = [executableSuccessorPipelines, nodeEngine](Runtime::TupleBuffer& buffer) {
        for (const auto& executableSuccessor : executableSuccessorPipelines) {
            NES_DEBUG("Emit buffer to query manager");
            nodeEngine->getQueryManager()->addWorkForNextPipeline(buffer, executableSuccessor);
        }
    };

    auto executionContext =
        std::make_shared<Runtime::Execution::PipelineExecutionContext>(subQueryPlanId,
                                                                       nodeEngine->getQueryManager(),
                                                                       nodeEngine->getBufferManager(),
                                                                       emitToSuccessorFunctionHandler,
                                                                       emitToQueryManagerFunctionHandler,
                                                                       executableOperator->getOperatorHandlers(),
                                                                       executableSuccessorPipelines.size());

    auto executablePipeline = Runtime::Execution::ExecutablePipeline::create(pipeline->getPipelineId(),
                                                                             subQueryPlanId,
                                                                             executionContext,
                                                                             executableOperator->getExecutablePipelineStage(),
                                                                             pipeline->getPredecessors().size(),
                                                                             executableSuccessorPipelines);

    executablePipelines.emplace_back(executablePipeline);
    return executablePipeline;
}

}// namespace NES::QueryCompilation