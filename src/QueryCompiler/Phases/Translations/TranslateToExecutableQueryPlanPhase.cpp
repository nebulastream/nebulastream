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
#include <NodeEngine/Execution/NewExecutablePipeline.hpp>
#include <NodeEngine/Execution/PipelineExecutionContext.hpp>
#include <NodeEngine/NodeEngine.hpp>
#include <Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/LogicalStreamSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceDescriptor.hpp>
#include <Phases/ConvertLogicalToPhysicalSink.hpp>
#include <Phases/ConvertLogicalToPhysicalSource.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Plans/Utils/QueryPlanIterator.hpp>
#include <QueryCompiler/Operators/ExecutableOperator.hpp>
#include <QueryCompiler/Operators/OperatorPipeline.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalSinkOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalSourceOperator.hpp>
#include <QueryCompiler/Operators/PipelineQueryPlan.hpp>
#include <QueryCompiler/Operators/PipelineQueryPlanIterator.hpp>
#include <QueryCompiler/Phases/Translations/GeneratableOperatorProvider.hpp>
#include <QueryCompiler/Phases/Translations/TranslateToExecutableQueryPlanPhase.hpp>
#include <QueryCompiler/QueryCompilerForwardDeclaration.hpp>
#include <variant>

namespace NES {
namespace QueryCompilation {

TranslateToExecutableQueryPlanPhasePtr TranslateToExecutableQueryPlanPhase::create() {
    return std::make_shared<TranslateToExecutableQueryPlanPhase>();
}

NodeEngine::Execution::NewExecutableQueryPlanPtr
TranslateToExecutableQueryPlanPhase::apply(PipelineQueryPlanPtr pipelineQueryPlan, NodeEngine::NodeEnginePtr nodeEngine) {
    std::vector<DataSourcePtr> sources;
    std::vector<DataSinkPtr> sinks;
    std::vector<NodeEngine::Execution::NewExecutablePipelinePtr> executablePipelines;
    std::map<uint64_t, NodeEngine::Execution::PredecessorPipeline> pipelineIdToPipelineMap;

    //Process all pipelines recursively.
    auto sourcePipelines = pipelineQueryPlan->getSourcePipelines();
    for (auto pipeline : sourcePipelines) {
        processSource(pipeline, sources, sinks, executablePipelines, nodeEngine, pipelineIdToPipelineMap);
    }

    return std::make_shared<NodeEngine::Execution::NewExecutableQueryPlan>(
        pipelineQueryPlan->getQueryId(), pipelineQueryPlan->getQuerySubPlanId(), std::move(sources), std::move(sinks),
        std::move(executablePipelines), nodeEngine->getQueryManager(), nodeEngine->getBufferManager());
}
NodeEngine::Execution::SuccessorPipeline TranslateToExecutableQueryPlanPhase::processSuccessor(
    OperatorPipelinePtr pipeline, std::vector<DataSourcePtr>& sources, std::vector<DataSinkPtr>& sinks,
    std::vector<NodeEngine::Execution::NewExecutablePipelinePtr>& executablePipelines, NodeEngine::NodeEnginePtr nodeEngine,
    std::map<uint64_t, NodeEngine::Execution::PredecessorPipeline> pipelineIdToPipelineMap) {

    if (pipeline->isSinkPipeline()) {
        return processSink(pipeline, sources, sinks, executablePipelines, nodeEngine, pipelineIdToPipelineMap);
    } else if (pipeline->isOperatorPipeline()) {
        return processOperatorPipeline(pipeline, sources, sinks, executablePipelines, nodeEngine, pipelineIdToPipelineMap);
    } else {
        NES_FATAL_ERROR("Should not reach here");
    }
}

void TranslateToExecutableQueryPlanPhase::processSource(
    OperatorPipelinePtr pipeline, std::vector<DataSourcePtr>& sources, std::vector<DataSinkPtr>& sinks,
    std::vector<NodeEngine::Execution::NewExecutablePipelinePtr>& executablePipelines, NodeEngine::NodeEnginePtr nodeEngine,
    std::map<uint64_t, NodeEngine::Execution::PredecessorPipeline> pipelineIdToPipelineMap) {

    if (!pipeline->isSourcePipeline()) {
        NES_ERROR("This is not a source pipeline");
    }

    auto rootOperator = pipeline->getQueryPlan()->getRootOperators()[0];
    auto sourceOperator = rootOperator->as<PhysicalOperators::PhysicalSourceOperator>();
    auto sourceDescriptor = sourceOperator->getSourceDescriptor();
    if (sourceDescriptor->instanceOf<LogicalStreamSourceDescriptor>()) {
        sourceDescriptor = nodeEngine->createLogicalSourceDescriptor(sourceDescriptor);
    }

    auto source = ConvertLogicalToPhysicalSource::createDataSource(sourceOperator->getId(), sourceDescriptor, nodeEngine, 64);
    pipelineIdToPipelineMap[pipeline->getPipelineId()] = source;
    sources.emplace_back(source);
    std::vector<NodeEngine::Execution::SuccessorPipeline> executableSuccessorPipelines;
    for (auto successor : pipeline->getSuccessors()) {
        auto executableSuccessor =
            processSuccessor(successor, sources, sinks, executablePipelines, nodeEngine, pipelineIdToPipelineMap);
        if (auto nextExecutablePipeline = std::get_if<NodeEngine::Execution::NewExecutablePipelinePtr>(&executableSuccessor)) {
            (*nextExecutablePipeline)->addPredecessor(source);
        }
    }
}

NodeEngine::Execution::SuccessorPipeline TranslateToExecutableQueryPlanPhase::processSink(
    OperatorPipelinePtr pipeline, std::vector<DataSourcePtr>& sources, std::vector<DataSinkPtr>& sinks,
    std::vector<NodeEngine::Execution::NewExecutablePipelinePtr>& executablePipelines, NodeEngine::NodeEnginePtr nodeEngine,
    std::map<uint64_t, NodeEngine::Execution::PredecessorPipeline> pipelineIdToPipelineMap) {
    auto rootOperator = pipeline->getQueryPlan()->getRootOperators()[0];
    auto sinkOperator = rootOperator->as<PhysicalOperators::PhysicalSinkOperator>();
    auto sink = ConvertLogicalToPhysicalSink::createDataSink(sinkOperator->getOutputSchema(), sinkOperator->getSinkDescriptor(),
                                                             nodeEngine, 2);
    sinks.emplace_back(sink);
    return sink;
}

NodeEngine::Execution::SuccessorPipeline TranslateToExecutableQueryPlanPhase::processOperatorPipeline(
    OperatorPipelinePtr pipeline, std::vector<DataSourcePtr>& sources, std::vector<DataSinkPtr>& sinks,
    std::vector<NodeEngine::Execution::NewExecutablePipelinePtr>& executablePipelines, NodeEngine::NodeEnginePtr nodeEngine,
    std::map<uint64_t, NodeEngine::Execution::PredecessorPipeline> pipelineIdToPipelineMap) {

    auto rootOperator = pipeline->getQueryPlan()->getRootOperators()[0];
    auto executableOperator = rootOperator->as<ExecutableOperator>();

    std::vector<NodeEngine::Execution::SuccessorPipeline> executableSuccessorPipelines;
    for (auto successor : pipeline->getSuccessors()) {
        auto executableSuccessor =
            processSuccessor(successor, sources, sinks, executablePipelines, nodeEngine, pipelineIdToPipelineMap);
        executableSuccessorPipelines.emplace_back(executableSuccessor);
    }

    auto emitToSuccessorFunctionHandler = [executableSuccessorPipelines](NodeEngine::TupleBuffer& buffer,
                                                                         NodeEngine::WorkerContextRef workerContext) {
        for (auto& executableSuccessor : executableSuccessorPipelines) {
            if (auto sink = std::get_if<DataSinkPtr>(&executableSuccessor)) {
                (*sink)->writeData(buffer, workerContext);
            } else if (auto nextExecutablePipeline =
                           std::get_if<NodeEngine::Execution::NewExecutablePipelinePtr>(&executableSuccessor)) {
                (*nextExecutablePipeline)->execute(buffer, workerContext);
            }
        }
    };

    auto emitToQueryManagerFunctionHandler = [executableSuccessorPipelines, nodeEngine](NodeEngine::TupleBuffer& buffer) {
        for (auto& executableSuccessor : executableSuccessorPipelines) {
            if (std::get_if<DataSinkPtr>(&executableSuccessor)) {
                NES_ERROR("QueryCompiler: we cant emit to a sink, if no worker context is provided");
            } else if (std::get_if<NodeEngine::Execution::NewExecutablePipelinePtr>(&executableSuccessor)) {
                // todo call query manager if new pipelines have been integrated
                NES_ERROR("Not Implemented yet");
                // nodeEngine->getQueryManager()->addWorkForNextPipeline(buffer, (*nextExecutablePipeline));
            }
        }
    };

    // todo get operator handlers.

    auto operatorHandlers = std::vector<NodeEngine::Execution::OperatorHandlerPtr>();

    auto executionContext = std::make_shared<NodeEngine::Execution::PipelineExecutionContext>(
        /*todo add QuerySubPlanId*/ -1, nodeEngine->getQueryManager(), nodeEngine->getBufferManager(),
        emitToSuccessorFunctionHandler, emitToQueryManagerFunctionHandler, operatorHandlers, executableSuccessorPipelines.size());

    auto executablePipeline = NodeEngine::Execution::NewExecutablePipeline::create(
        pipeline->getPipelineId(), /*todo add QuerySubPlanId*/ -1, executionContext,
        executableOperator->getExecutablePipelineStage(), std::vector<NodeEngine::Execution::PredecessorPipeline>(),
        std::vector<NodeEngine::Execution::SuccessorPipeline>());

    for (auto executableSuccessor : executableSuccessorPipelines) {
        if (auto(executableSuccessorPipeline) =
                std::get_if<NodeEngine::Execution::NewExecutablePipelinePtr>(&executableSuccessor)) {
            executableSuccessorPipeline->get()->addPredecessor(executablePipeline);
        }
        executablePipeline->addSuccessor(executableSuccessor);
    }
    executablePipelines.emplace_back(executablePipeline);
    return executablePipeline;
}

}// namespace QueryCompilation
}// namespace NES