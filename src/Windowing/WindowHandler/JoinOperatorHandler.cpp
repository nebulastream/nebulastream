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
#include <Windowing/WindowHandler/AbstractJoinHandler.hpp>
#include <Windowing/WindowHandler/JoinOperatorHandler.hpp>
namespace NES::Join {

JoinOperatorHandlerPtr JoinOperatorHandler::create(LogicalJoinDefinitionPtr joinDefinition, SchemaPtr resultSchema,
                                                   AbstractJoinHandlerPtr joinHandler) {
    return std::make_shared<JoinOperatorHandler>(joinDefinition, resultSchema, joinHandler);
}

JoinOperatorHandlerPtr JoinOperatorHandler::create(LogicalJoinDefinitionPtr joinDefinition, SchemaPtr resultSchema) {
    return std::make_shared<JoinOperatorHandler>(joinDefinition, resultSchema);
}

JoinOperatorHandler::JoinOperatorHandler(LogicalJoinDefinitionPtr joinDefinition, SchemaPtr resultSchema)
    : joinDefinition(joinDefinition), resultSchema(resultSchema) {}

JoinOperatorHandler::JoinOperatorHandler(LogicalJoinDefinitionPtr joinDefinition, SchemaPtr resultSchema,
                                         AbstractJoinHandlerPtr joinHandler)
    : joinDefinition(joinDefinition), joinHandler(joinHandler), resultSchema(resultSchema) {}

LogicalJoinDefinitionPtr JoinOperatorHandler::getJoinDefinition() { return joinDefinition; }

void JoinOperatorHandler::setJoinHandler(AbstractJoinHandlerPtr joinHandler) { this->joinHandler = joinHandler; }

SchemaPtr JoinOperatorHandler::getResultSchema() { return resultSchema; }
void JoinOperatorHandler::start(NodeEngine::Execution::PipelineExecutionContextPtr) { joinHandler->start(); }
void JoinOperatorHandler::stop(NodeEngine::Execution::PipelineExecutionContextPtr) { joinHandler->stop(); }
}// namespace NES::Join