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

#include "IREEAdapter.hpp"
#include "IREEInferenceOperatorHandler.hpp"
#include <Util/Logger/Logger.hpp>

namespace NES::Runtime::Execution::Operators
{

IREEInferenceOperatorHandler::IREEInferenceOperatorHandler(const std::string& model)
{
    this->model = model;
    ireeAdapter = IREEAdapter::create();
    ireeAdapter->initializeModel(model);
}

void IREEInferenceOperatorHandler::start(PipelineExecutionContext& pipelineExecutionContext, uint32_t localStateVariableId) {}
void IREEInferenceOperatorHandler::stop(Runtime::QueryTerminationType terminationType, PipelineExecutionContext& pipelineExecutionContext) {}

const std::string& IREEInferenceOperatorHandler::getModel() const { return model; }
const std::shared_ptr<IREEAdapter>& IREEInferenceOperatorHandler::getIREEAdapter() const { return ireeAdapter; }

}