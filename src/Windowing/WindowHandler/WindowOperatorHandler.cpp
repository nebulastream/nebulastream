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
#include <Windowing/WindowHandler/WindowOperatorHandler.hpp>
#include <Windowing/WindowHandler/AbstractWindowHandler.hpp>
namespace NES::Windowing{

NodeEngine::Execution::OperatorHandlerPtr WindowOperatorHandler::create(LogicalWindowDefinitionPtr windowDefinition, SchemaPtr resultSchema) {
    return std::make_shared<WindowOperatorHandler>(windowDefinition, resultSchema);
}

WindowOperatorHandler::WindowOperatorHandler(LogicalWindowDefinitionPtr windowDefinition, SchemaPtr resultSchema): windowDefinition(windowDefinition), resultSchema(resultSchema) {

}

LogicalWindowDefinitionPtr WindowOperatorHandler::getWindowDefinition() {
    return windowDefinition;
}

void WindowOperatorHandler::setWindowHandler(AbstractWindowHandlerPtr windowHandler) {
    this->windowHandler = windowHandler;
}

SchemaPtr WindowOperatorHandler::getResultSchema() {
    return resultSchema;
}
void WindowOperatorHandler::start(NodeEngine::Execution::PipelineExecutionContextPtr) {
    windowHandler->start();
}
void WindowOperatorHandler::stop(NodeEngine::Execution::PipelineExecutionContextPtr) {
    windowHandler->stop();
}
}