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
#include <utility>
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
void BatchJoinOperatorHandler::start(Runtime::Execution::PipelineExecutionContextPtr,
                                 [[maybe_unused]] Runtime::StateManagerPtr stateManager,
                                [[maybe_unused]] uint32_t localStateVariableId) {
    if (batchJoinHandler) {
//        batchJoinHandler->start(stateManager, localStateVariableId); todo jm
    }
}

void BatchJoinOperatorHandler::stop(Runtime::Execution::PipelineExecutionContextPtr) {
    if (batchJoinHandler) {
        //        batchJoinHandler->stop(); todo jm
    }
}
void BatchJoinOperatorHandler::reconfigure(Runtime::ReconfigurationMessage& task, Runtime::WorkerContext& context) {
    Runtime::Execution::OperatorHandler::reconfigure(task, context);
    //    batchJoinHandler->reconfigure(task, context); todo jm
}

void BatchJoinOperatorHandler::postReconfigurationCallback(Runtime::ReconfigurationMessage& task) {
    Runtime::Execution::OperatorHandler::postReconfigurationCallback(task);
    //    batchJoinHandler->postReconfigurationCallback(task); todo jm
}

}// namespace NES::Join