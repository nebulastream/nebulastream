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
#include <State/StateManager.hpp>
#include <Windowing/WindowHandler/AbstractJoinHandler.hpp>
#include <Windowing/WindowHandler/JoinOperatorHandler.hpp>
#include <utility>
namespace NES::Join {

JoinOperatorHandlerPtr JoinOperatorHandler::create(const LogicalJoinDefinitionPtr& joinDefinition,
                                                   const SchemaPtr& resultSchema,
                                                   const AbstractJoinHandlerPtr& joinHandler) {
    return std::make_shared<JoinOperatorHandler>(joinDefinition, resultSchema, joinHandler);
}

JoinOperatorHandlerPtr JoinOperatorHandler::create(const LogicalJoinDefinitionPtr& joinDefinition,
                                                   const SchemaPtr& resultSchema) {
    return std::make_shared<JoinOperatorHandler>(joinDefinition, resultSchema);
}

JoinOperatorHandler::JoinOperatorHandler(LogicalJoinDefinitionPtr joinDefinition, SchemaPtr resultSchema)
    : joinDefinition(std::move(joinDefinition)), resultSchema(std::move(resultSchema)) {
    NES_DEBUG("JoinOperatorHandler(LogicalJoinDefinitionPtr joinDefinition, SchemaPtr resultSchema)");
}

JoinOperatorHandler::JoinOperatorHandler(LogicalJoinDefinitionPtr joinDefinition,
                                         SchemaPtr resultSchema,
                                         AbstractJoinHandlerPtr joinHandler)
    : joinDefinition(std::move(joinDefinition)), joinHandler(std::move(joinHandler)), resultSchema(std::move(resultSchema)) {
    NES_DEBUG("JoinOperatorHandler(LogicalJoinDefinitionPtr joinDefinition, SchemaPtr resultSchema, AbstractJoinHandlerPtr "
              "joinHandler)");
}

LogicalJoinDefinitionPtr JoinOperatorHandler::getJoinDefinition() { return joinDefinition; }

void JoinOperatorHandler::setJoinHandler(AbstractJoinHandlerPtr joinHandler) { this->joinHandler = std::move(joinHandler); }

SchemaPtr JoinOperatorHandler::getResultSchema() { return resultSchema; }
void JoinOperatorHandler::start(Runtime::Execution::PipelineExecutionContextPtr,
                                Runtime::StateManagerPtr stateManager,
                                uint32_t localStateVariableId) {
    if (joinHandler) {
        joinHandler->start(stateManager, localStateVariableId);
    }
}

void JoinOperatorHandler::stop(Runtime::Execution::PipelineExecutionContextPtr) {
    if (joinHandler) {
        joinHandler->stop();
    }
}
void JoinOperatorHandler::reconfigure(Runtime::ReconfigurationMessage& task, Runtime::WorkerContext& context) {
    Runtime::Execution::OperatorHandler::reconfigure(task, context);
    joinHandler->reconfigure(task, context);
}

void JoinOperatorHandler::postReconfigurationCallback(Runtime::ReconfigurationMessage& task) {
    Runtime::Execution::OperatorHandler::postReconfigurationCallback(task);
    joinHandler->postReconfigurationCallback(task);
}

}// namespace NES::Join