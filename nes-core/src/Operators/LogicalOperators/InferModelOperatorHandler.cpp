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

#ifdef INFERENCE_OPERATOR_DEF

#include <Execution/Operators/Streaming/InferModel/InferenceAdapter.hpp>
#include <Execution/Operators/Streaming/InferModel/ONNXAdapter.hpp>
#include <Execution/Operators/Streaming/InferModel/TensorflowAdapter.hpp>
#include <Operators/LogicalOperators/InferModelOperatorHandler.hpp>
#include <Runtime/Reconfigurable.hpp>
#include <Runtime/WorkerContext.hpp>
#include <State/StateManager.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::InferModel {

InferModelOperatorHandlerPtr InferModelOperatorHandler::create(const std::string& model) {
    return std::make_shared<InferModelOperatorHandler>(model);
}

InferModelOperatorHandler::InferModelOperatorHandler(const std::string& model) {
    this->model = model;
    if (model.ends_with(".tflite")) {
#ifdef TFDEF
        adapter = Runtime::Execution::Operators::TensorflowAdapter::create();
#else
        throw std::runtime_error("TensorflowAdapter not supported, Compile with NES_USE_TF");
#endif
    } else if (model.ends_with(".onnx")) {
#ifdef ONNXDEF
        adapter = Runtime::Execution::Operators::ONNXAdapter::create();
#else
        throw std::runtime_error("ONNXAdapter not supported, Compile with NES_USE_ONNX");
#endif
    }
    adapter->initializeModel(model);
}

void InferModelOperatorHandler::start(Runtime::Execution::PipelineExecutionContextPtr,
                                      Runtime::StateManagerPtr stateManager,
                                      uint32_t localStateVariableId) {
    NES_DEBUG("nodeId: {}", stateManager->getNodeId());
    NES_DEBUG("localStateVaribaleId: {}", localStateVariableId);
}

void InferModelOperatorHandler::stop(Runtime::QueryTerminationType, Runtime::Execution::PipelineExecutionContextPtr) {}

void InferModelOperatorHandler::reconfigure(Runtime::ReconfigurationMessage& task, Runtime::WorkerContext& context) {
    Runtime::Execution::OperatorHandler::reconfigure(task, context);
}

void InferModelOperatorHandler::postReconfigurationCallback(Runtime::ReconfigurationMessage& task) {
    Runtime::Execution::OperatorHandler::postReconfigurationCallback(task);
}

const std::string& InferModelOperatorHandler::getModel() const { return model; }

Runtime::Execution::Operators::InferenceAdapter* InferModelOperatorHandler::getTensorflowAdapter() const { return adapter.get(); }

}// namespace NES::InferModel

#endif// INFERENCE_OPERATOR_DEF
