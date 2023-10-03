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

#include <Execution/Operators/ONNX/ONNXInferenceOperatorHandler.hpp>
#include <Execution/Operators/ONNX/ONNXAdapter.hpp>
#include <Runtime/WorkerContext.hpp>

namespace NES::Runtime::Execution::Operators {

    ONNXInferenceOperatorHandlerPtr
    ONNXInferenceOperatorHandler::create(const std::string &model) { return std::make_shared<ONNXInferenceOperatorHandler>(model); }

    ONNXInferenceOperatorHandler::ONNXInferenceOperatorHandler(const std::string &model) {
        this->model = model;

        if (model.ends_with(".onnx")) {
            onnxAdapter = ONNXAdapter::create();
        } else {
            throw std::runtime_error("Model file must end with .onnx");
        }
        onnxAdapter->initializeModel(model);
    }

    void
    ONNXInferenceOperatorHandler::start(Runtime::Execution::PipelineExecutionContextPtr, Runtime::StateManagerPtr, uint32_t) {}

    void ONNXInferenceOperatorHandler::stop(Runtime::QueryTerminationType, Runtime::Execution::PipelineExecutionContextPtr) {}

    void ONNXInferenceOperatorHandler::reconfigure(Runtime::ReconfigurationMessage &, Runtime::WorkerContext &) {}

    void ONNXInferenceOperatorHandler::postReconfigurationCallback(Runtime::ReconfigurationMessage &) {}

    const std::string &ONNXInferenceOperatorHandler::getModel() const { return model; }

    const ONNXAdapterPtr &ONNXInferenceOperatorHandler::getONNXAdapter() const { return onnxAdapter; }

}// namespace NES::Runtime::Execution::Operators