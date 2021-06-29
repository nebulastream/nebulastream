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
#include <Windowing/WindowHandler/InferModelOperatorHandler.hpp>
#include <NodeEngine/Reconfigurable.hpp>
#include <NodeEngine/WorkerContext.hpp>
#include <Windowing/JoinForwardRefs.hpp>
#include <Windowing/Runtime/WindowSliceStore.hpp>
#include <Windowing/Runtime/WindowState.hpp>
#include <Windowing/WindowActions/BaseExecutableWindowAction.hpp>


namespace NES::Join {

InferModelOperatorHandlerPtr InferModelOperatorHandler::create(std::string model) {
    return std::make_shared<InferModelOperatorHandler>(model);
}

InferModelOperatorHandler::InferModelOperatorHandler(std::string model) {
    this->model = model;
    tfAdapter = TensorflowAdapter::create();
    tfAdapter->initializeModel(model);
}

void InferModelOperatorHandler::start(NodeEngine::Execution::PipelineExecutionContextPtr,
                                NodeEngine::StateManagerPtr stateManager,
                                uint32_t localStateVariableId) {
    std::cout << stateManager->getNodeId() << std::endl;
    std::cout << localStateVariableId << std::endl;


    // implement start here
}

void InferModelOperatorHandler::stop(NodeEngine::Execution::PipelineExecutionContextPtr) {
    // implement stop here
}
void InferModelOperatorHandler::reconfigure(NodeEngine::ReconfigurationMessage& task, NodeEngine::WorkerContext& context) {
    NodeEngine::Execution::OperatorHandler::reconfigure(task, context);
}

void InferModelOperatorHandler::postReconfigurationCallback(NodeEngine::ReconfigurationMessage& task) {
    NodeEngine::Execution::OperatorHandler::postReconfigurationCallback(task);
}
const std::string& InferModelOperatorHandler::getModel() const { return model; }
const TensorflowAdapterPtr& InferModelOperatorHandler::getTensorflowAdapter() const { return tfAdapter; }

}// namespace NES::Join