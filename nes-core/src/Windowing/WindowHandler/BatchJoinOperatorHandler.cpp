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
#include <State/StateManager.hpp>
#include <Windowing/WindowHandler/AbstractJoinHandler.hpp>
#include <Windowing/WindowHandler/BatchJoinOperatorHandler.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <utility>
#include <variant>
#include <Sources/StaticDataSource.hpp>

namespace NES::Join {

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
    : batchJoinDefinition(std::move(batchJoinDefinition)), batchJoinHandler(std::move(batchJoinHandler)), resultSchema(std::move(resultSchema)) {
    NES_DEBUG("BatchJoinOperatorHandler(LogicalBatchJoinDefinitionPtr batchJoinDefinition, SchemaPtr resultSchema, AbstractBatchJoinHandlerPtr "
              "batchJoinHandler)");
}

LogicalBatchJoinDefinitionPtr BatchJoinOperatorHandler::getBatchJoinDefinition() { return batchJoinDefinition; }

void BatchJoinOperatorHandler::setBatchJoinHandler(AbstractBatchJoinHandlerPtr batchJoinHandler) { this->batchJoinHandler = std::move(batchJoinHandler); }

SchemaPtr BatchJoinOperatorHandler::getResultSchema() { return resultSchema; }
void BatchJoinOperatorHandler::start(Runtime::Execution::PipelineExecutionContextPtr context,
                                     Runtime::StateManagerPtr,
                                     uint32_t) {
    // TODO the purpose of this code is to collect pointers to the two precessor pipelines that lie before Build&Probe pipelines
    // TODO but it only works with simple query plans, with static data sources as precessors

    // TODO further, we want a general model for pipeline predecessor access.
    // TODO most functions in this file are quite the hack.

    NES_DEBUG("BatchJoinOperatorHandler::start: Analyzing pipeline. # predecessors of pipeline: " << context->getPredecessors().size());
    if (context->getPredecessors().size() == 1) {
        NES_DEBUG("Build pipeline!");
        if (foundAndStartedBuildSide) {
            NES_DEBUG("BatchJoinOperatorHandler::start() was called a second time on a build pipeline.");
            // this currently happens all the time, because we register bjoHandler twice in one build pipeline
            // todo change this to an error when we fixed this.
        } else {
            this->buildPredecessor = context->getPredecessors()[0];
            if (const auto* source = std::get_if<std::weak_ptr<NES::DataSource>>(&(context->getPredecessors()[0]))) {
                NES_DEBUG("BatchJoinOperatorHandler: Found Build Source in predecessors of Build pipeline. "
                          "Starting it now.");
                NES::Runtime::StartSourceEvent event;
                source->lock()->onEvent(event);
                foundAndStartedBuildSide = true;
            } else {
                NES_ERROR("BatchJoinOperatorHandler: In this pattern the predecessor should be a DataSource!");
            }
        }

        return;
    }
    if (context->getPredecessors().size() == 2) {
        NES_DEBUG("Probe pipeline!");
        if (foundProbeSide) {
            NES_ERROR("BatchJoinOperatorHandler::start() was called a second time on a probe pipeline");
        }
        // one of the predecessors will be the Batch Join Build Pipeline
        // we want to get a pointer to ther other predecessor
        for (auto predecessor : context->getPredecessors()) {
            if (const auto* source = std::get_if<std::weak_ptr<NES::DataSource>>(&predecessor)) {
                NES_DEBUG("BatchJoinOperatorHandler: Found Probe Source in predecessors of Probe pipeline.");
                this->probePredecessor = predecessor;
                foundProbeSide = true;

                return;
            }
            // if not, this must be the build pipeline, we don't care about it here
        }
        // one of the precessors should have fulfilled the if statement
        NES_FATAL_ERROR("BatchJoinOperatorHandler: In this pattern one predecessor should be a DataSource!");
        return;
    }

    NES_FATAL_ERROR("BatchJoinOperatorHandler::start() called with pipeline that didn't have one or two successors!");
    return;
}

void BatchJoinOperatorHandler::stop(Runtime::Execution::PipelineExecutionContextPtr) {
//    todo jm: anything necessary here? will the join handler and hash table destruct themselves
}
void BatchJoinOperatorHandler::reconfigure(Runtime::ReconfigurationMessage& task, Runtime::WorkerContext& context) {
    Runtime::Execution::OperatorHandler::reconfigure(task, context);
}

void BatchJoinOperatorHandler::postReconfigurationCallback(Runtime::ReconfigurationMessage& task) {
    Runtime::Execution::OperatorHandler::postReconfigurationCallback(task);
    //  todo jm: I think the EoS message from the build source still gets passed upstream as the JoinBuildPipeline has the JoinProbePipeline as a parent

    // on first call, this should send a message to probe child (right) to start sending data
    // on second call (EoS from probe side) nothing has to be done
    if (startedProbeSide) {
        NES_ERROR("BatchJoinOperatorHandler::postReconfigurationCallback: Abort attempt to start Probe Source twice.");
        return;
    }

    const auto* source = std::get_if<std::weak_ptr<NES::DataSource>>(&probePredecessor);
    NES_DEBUG("BatchJoinOperatorHandler::postReconfigurationCallback:"
              "The Build side finished processing. Starting the Probe Source now.");
    NES::Runtime::StartSourceEvent event;
    source->lock()->onEvent(event);
    startedProbeSide = true;
}

}// namespace NES::Join