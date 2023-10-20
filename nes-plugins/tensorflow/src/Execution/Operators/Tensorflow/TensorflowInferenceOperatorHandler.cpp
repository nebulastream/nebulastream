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

#include <Execution/Operators/Tensorflow/TensorflowInferenceOperatorHandler.hpp>
#include <Execution/Operators/Tensorflow/TensorflowAdapter.hpp>
#include <Runtime/WorkerContext.hpp>

namespace NES::Runtime::Execution::Operators {

TensorflowInferenceOperatorHandlerPtr TensorflowInferenceOperatorHandler::create(const std::string& model) { return std::make_shared<TensorflowInferenceOperatorHandler>(model); }

TensorflowInferenceOperatorHandler::TensorflowInferenceOperatorHandler(const std::string& model) {
    this->model = model;
    tfAdapter = TensorflowAdapter::create();
    tfAdapter->initializeModel(model);
}

void TensorflowInferenceOperatorHandler::start(Runtime::Execution::PipelineExecutionContextPtr,  uint32_t) {}

void TensorflowInferenceOperatorHandler::stop(Runtime::QueryTerminationType, Runtime::Execution::PipelineExecutionContextPtr) {}

void TensorflowInferenceOperatorHandler::reconfigure(Runtime::ReconfigurationMessage&, Runtime::WorkerContext&) {}

void TensorflowInferenceOperatorHandler::postReconfigurationCallback(Runtime::ReconfigurationMessage&) {}

const std::string& TensorflowInferenceOperatorHandler::getModel() const { return model; }

const TensorflowAdapterPtr& TensorflowInferenceOperatorHandler::getTensorflowAdapter() const { return tfAdapter; }

}// namespace NES::Runtime::Execution::Operators
