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
#include <QueryCompiler/Operators/PhysicalOperators/CEP/CEPOperatorHandler/CEPOperatorHandler.hpp>

namespace NES::CEP {

CEPOperatorHandlerPtr CEPOperatorHandler::create() {
    return std::make_shared<NES::CEP::CEPOperatorHandler>();
}

void CEPOperatorHandler::start(Runtime::Execution::PipelineExecutionContextPtr context, Runtime::StateManagerPtr stateManager, uint32_t localStateVariableId) {
    NES_DEBUG("CEPOperatorHandler::start() with localStateVariableId" << localStateVariableId << context);
    this->stateManager = stateManager;
    this->clearCounter();
    StateId stateId = {stateManager->getNodeId(), id, localStateVariableId};
    //TODO fix registration -> key,Value
    //stateManager->registerState<CEPOperatorHandler>(stateId);
}

void CEPOperatorHandler::stop(Runtime::Execution::PipelineExecutionContextPtr context) {

   //TODO: inregister from stateManager (requires StateVariable, ?localStateVariableId?) stateManager->unRegisterState(STATEVARIABLE)
}

void CEPOperatorHandler::reconfigure(Runtime::ReconfigurationMessage& task, Runtime::WorkerContext& context) {
    Reconfigurable::reconfigure(task, context);
}

void CEPOperatorHandler::postReconfigurationCallback(Runtime::ReconfigurationMessage& task) {
    Reconfigurable::postReconfigurationCallback(task);
}

void CEPOperatorHandler::addTuple(){
    counter++;
}

uint64_t CEPOperatorHandler::getCounter(){
    return counter;
}
void CEPOperatorHandler::clearCounter(){
    counter = 0;
}

}// namespace NES::CEP