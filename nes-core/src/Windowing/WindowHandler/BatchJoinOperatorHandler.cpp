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
#include <Runtime/Execution/ExecutablePipeline.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/QueryManager.hpp>
#include <Sources/StaticDataSource.hpp>
#include <State/StateManager.hpp>
#include <Windowing/WindowHandler/AbstractJoinHandler.hpp>
#include <Windowing/WindowHandler/BatchJoinOperatorHandler.hpp>
#include <utility>
#include <variant>

namespace NES::Join::Experimental {

BatchJoinOperatorHandlerPtr BatchJoinOperatorHandler::create(const LogicalBatchJoinDefinitionPtr& batchJoinDefinition,
                                                             const SchemaPtr& resultSchema,
                                                             const AbstractBatchJoinHandlerPtr& batchJoinHandler) {
    return std::make_shared<BatchJoinOperatorHandler>(batchJoinDefinition, resultSchema, batchJoinHandler);
}

BatchJoinOperatorHandlerPtr BatchJoinOperatorHandler::create(const LogicalBatchJoinDefinitionPtr& batchJoinDefinition,
                                                             const SchemaPtr& resultSchema) {
    return std::make_shared<BatchJoinOperatorHandler>(batchJoinDefinition, resultSchema);
}

BatchJoinOperatorHandler::BatchJoinOperatorHandler(LogicalBatchJoinDefinitionPtr batchJoinDefinition, SchemaPtr resultSchema)
    : batchJoinDefinition(std::move(batchJoinDefinition)), resultSchema(std::move(resultSchema)) {
    NES_DEBUG("BatchJoinOperatorHandler(LogicalBatchJoinDefinitionPtr batchJoinDefinition, SchemaPtr resultSchema)");
}

BatchJoinOperatorHandler::BatchJoinOperatorHandler(LogicalBatchJoinDefinitionPtr batchJoinDefinition,
                                                   SchemaPtr resultSchema,
                                                   AbstractBatchJoinHandlerPtr batchJoinHandler)
    : batchJoinDefinition(std::move(batchJoinDefinition)), batchJoinHandler(std::move(batchJoinHandler)),
      resultSchema(std::move(resultSchema)) {
    NES_DEBUG("BatchJoinOperatorHandler(LogicalBatchJoinDefinitionPtr batchJoinDefinition, SchemaPtr resultSchema, "
              "AbstractBatchJoinHandlerPtr "
              "batchJoinHandler)");
}

LogicalBatchJoinDefinitionPtr BatchJoinOperatorHandler::getBatchJoinDefinition() { return batchJoinDefinition; }

void BatchJoinOperatorHandler::setBatchJoinHandler(AbstractBatchJoinHandlerPtr batchJoinHandler) {
    this->batchJoinHandler = std::move(batchJoinHandler);
}

SchemaPtr BatchJoinOperatorHandler::getResultSchema() { return resultSchema; }

void BatchJoinOperatorHandler::start(Runtime::Execution::PipelineExecutionContextPtr context,
                                     Runtime::StateManagerPtr,
                                     uint32_t) {
    NES_ASSERT(this->buildPipelineID != 0, "BatchJoinOperatorHandler: The Build Pipeline is not registered at start().");
    NES_ASSERT(this->probePipelineID != 0, "BatchJoinOperatorHandler: The Probe Pipeline is not registered at start().");

    NES_DEBUG(
        "BatchJoinOperatorHandler::start: Analyzing pipeline. # predecessors of pipeline: " << context->getPredecessors().size());
    if (context->getPipelineID() == this->buildPipelineID) {
        NES_DEBUG("BatchJoinOperatorHandler: Identified build pipeline by ID.");
        NES_ASSERT(context->getPredecessors().size() >= 1,
                   "BatchJoinOperatorHandler: The Build Pipeline should have at least one predecessor.");

        if (foundAndStartedBuildSide) {
            NES_THROW_RUNTIME_ERROR("BatchJoinOperatorHandler::start() was called a second time on a build pipeline.");
            return;
        }

        // send start source event to all build side sources.
        NES::Runtime::StartSourceEvent event;
        this->buildPredecessors = context->getPredecessors();

        // todo jm we can't run queries where the Build-side StaticDataSource is not on the same node as the Build Pipeline.
        // because we have no access to the workerContext here, and can't call onEvent(event, workerContext).

        for (auto buildPredecessor : this->buildPredecessors) {
            if (const auto* sourcePredecessor = std::get_if<std::weak_ptr<NES::DataSource>>(&(buildPredecessor))) {
                NES_DEBUG("BatchJoinOperatorHandler: Found Build Source in predecessors of Build pipeline. "
                          "Starting it now.");
                sourcePredecessor->lock()->onEvent(event);
            } else if (const auto* pipelinePredecessor =
                           std::get_if<std::weak_ptr<NES::Runtime::Execution::ExecutablePipeline>>(&(buildPredecessor))) {
                NES_DEBUG("BatchJoinOperatorHandler: Found Pipeline in predecessors of Build pipeline. "
                          "Forwarding the start source event now.");
                pipelinePredecessor->lock()->onEvent(event);
            }
        }

        this->foundAndStartedBuildSide = true;
        // we expect one EoS message & on call of reconfigure() per logical predecessor and per thread.
        uint16_t threadsOnWorker = context->getNumberOfWorkerThreads();
        this->workerThreadsBuildSideTotal = this->buildPredecessors.size() * threadsOnWorker;
        return;
    }
    if (context->getPipelineID() == this->probePipelineID) {
        NES_DEBUG("BatchJoinOperatorHandler: Identified probe pipeline by ID.");
        NES_ASSERT(
            context->getPredecessors().size() >= 2,
            "BatchJoinOperatorHandler: The Probe Pipeline should have the Build Pipeline and at least one other predecessor.");
        NES_ASSERT(!foundProbeSide, "BatchJoinOperatorHandler::start() was called a second time on a probe pipeline");

        // one of the predecessors will be the Batch Join Build Pipeline
        // we want to get a pointer to the other predecessor
        bool encounteredBuildPipeline = false;// (within the probe pipelines predecessors)
        for (auto probePredecessor : context->getPredecessors()) {
            if (const auto* sourcePredecessor = std::get_if<std::weak_ptr<NES::DataSource>>(&probePredecessor)) {
                NES_DEBUG("BatchJoinOperatorHandler: Found Probe Source in predecessors of Probe pipeline.");
                this->probePredecessors.push_back(probePredecessor);
            } else if (const auto* pipelinePredecessor =
                           std::get_if<std::weak_ptr<NES::Runtime::Execution::ExecutablePipeline>>(&probePredecessor)) {
                if (pipelinePredecessor->lock()->getPipelineId() == this->buildPipelineID) {
                    NES_ASSERT(
                        !encounteredBuildPipeline,
                        "BatchJoinOperatorHandler: More than one Predecessors of Probe Pipeline has ID of Build pipeline.");
                    encounteredBuildPipeline = true;
                    continue;// we do not save the Build Pipeline as a probe predecessor
                }

                NES_DEBUG("BatchJoinOperatorHandler: Found Pipeline in predecessors of Probe pipeline.");
                this->probePredecessors.push_back(probePredecessor);
            }
        }
        // one of the predecessors should have been the build side
        NES_ASSERT(encounteredBuildPipeline,
                   "BatchJoinOperatorHandler: One Predecessors of Probe Pipeline should have ID of Build pipeline.");
        return;
    }

    NES_FATAL_ERROR("BatchJoinOperatorHandler::start() called with pipeline that didn't have one or two successors!");
    return;
}

void BatchJoinOperatorHandler::stop(Runtime::QueryTerminationType, Runtime::Execution::PipelineExecutionContextPtr) {
    // nop
}

void BatchJoinOperatorHandler::reconfigure(Runtime::ReconfigurationMessage& task, Runtime::WorkerContext& context) {
    Runtime::Execution::OperatorHandler::reconfigure(task, context);
    if (!startedProbeSide && ++workerThreadsBuildSideFinished == workerThreadsBuildSideTotal) {
        startProbeSide(context);
    }
}

void BatchJoinOperatorHandler::startProbeSide(Runtime::WorkerContext& context) {
    NES_DEBUG("BatchJoinOperatorHandler: Starting Probe Predecessors.");
    NES::Runtime::StartSourceEvent event;

    for (auto probePredecessor : this->probePredecessors) {
        if (const auto* sourcePredecessor = std::get_if<std::weak_ptr<NES::DataSource>>(&probePredecessor)) {
            NES_DEBUG("BatchJoinOperatorHandler: Starting Probe Source predecessor.");
            sourcePredecessor->lock()->onEvent(event, context);
        } else if (const auto* pipelinePredecessor =
                       std::get_if<std::weak_ptr<NES::Runtime::Execution::ExecutablePipeline>>(&probePredecessor)) {
            NES_DEBUG("BatchJoinOperatorHandler: Send Start Source event to Pipeline (a Probe Side Predecessor).");
            pipelinePredecessor->lock()->onEvent(event, context);
        }
    }
    startedProbeSide = true;
}

void BatchJoinOperatorHandler::postReconfigurationCallback(Runtime::ReconfigurationMessage& task) {
    Runtime::Execution::OperatorHandler::postReconfigurationCallback(task);
    //  todo jm: I think the EoS message from the build source still gets passed upstream as the JoinBuildPipeline has the JoinProbePipeline as a parent
}

}// namespace NES::Join::Experimental