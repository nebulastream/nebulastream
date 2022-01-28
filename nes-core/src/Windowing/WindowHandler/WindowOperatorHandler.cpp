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
#include <Windowing/WindowHandler/AbstractWindowHandler.hpp>
#include <Windowing/WindowHandler/WindowOperatorHandler.hpp>
#include <utility>
namespace NES::Windowing {

WindowOperatorHandlerPtr WindowOperatorHandler::create(const LogicalWindowDefinitionPtr& windowDefinition,
                                                       const SchemaPtr& resultSchema,
                                                       const AbstractWindowHandlerPtr& windowHandler) {
    return std::make_shared<WindowOperatorHandler>(windowDefinition, resultSchema, windowHandler);
}

WindowOperatorHandlerPtr WindowOperatorHandler::create(const LogicalWindowDefinitionPtr& windowDefinition,
                                                       const SchemaPtr& resultSchema) {
    return std::make_shared<WindowOperatorHandler>(windowDefinition, resultSchema);
}

WindowOperatorHandler::WindowOperatorHandler(LogicalWindowDefinitionPtr windowDefinition, SchemaPtr resultSchema)
    : windowDefinition(std::move(windowDefinition)), resultSchema(std::move(resultSchema)) {}

WindowOperatorHandler::WindowOperatorHandler(LogicalWindowDefinitionPtr windowDefinition,
                                             SchemaPtr resultSchema,
                                             AbstractWindowHandlerPtr windowHandler)
    : windowDefinition(std::move(windowDefinition)), windowHandler(std::move(windowHandler)),
      resultSchema(std::move(resultSchema)) {}

LogicalWindowDefinitionPtr WindowOperatorHandler::getWindowDefinition() { return windowDefinition; }

void WindowOperatorHandler::setWindowHandler(AbstractWindowHandlerPtr windowHandler) {
    this->windowHandler = std::move(windowHandler);
}

SchemaPtr WindowOperatorHandler::getResultSchema() { return resultSchema; }

void WindowOperatorHandler::start(Runtime::Execution::PipelineExecutionContextPtr,
                                  Runtime::StateManagerPtr stateManager,
                                  uint32_t localStateVariableId) {
    windowHandler->start(stateManager, localStateVariableId);
}
void WindowOperatorHandler::stop(Runtime::Execution::PipelineExecutionContextPtr) { windowHandler->stop(); }
void WindowOperatorHandler::reconfigure(Runtime::ReconfigurationMessage& task, Runtime::WorkerContext& context) {
    Reconfigurable::reconfigure(task, context);
    windowHandler->reconfigure(task, context);
}
void WindowOperatorHandler::postReconfigurationCallback(Runtime::ReconfigurationMessage& task) {
    Reconfigurable::postReconfigurationCallback(task);
    windowHandler->postReconfigurationCallback(task);
}

}// namespace NES::Windowing