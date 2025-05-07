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

#include "IREEInferenceOperatorHandler.hpp"
#include <Util/Logger/Logger.hpp>
#include "IREEAdapter.hpp"

namespace NES::Runtime::Execution::Operators
{

IREEInferenceOperatorHandler::IREEInferenceOperatorHandler(Nebuli::Inference::Model model)
    : model(std::move(model)), ireeAdapter(IREEAdapter::create())
{
}

void IREEInferenceOperatorHandler::start(PipelineExecutionContext&, uint32_t)
{
    ireeAdapter->initializeModel(model);
}
void IREEInferenceOperatorHandler::stop(Runtime::QueryTerminationType, PipelineExecutionContext&)
{
}

const Nebuli::Inference::Model& IREEInferenceOperatorHandler::getModel() const
{
    return model;
}

const std::shared_ptr<IREEAdapter>& IREEInferenceOperatorHandler::getIREEAdapter() const
{
    return ireeAdapter;
}

}
