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
#include <PipelineExecutionContext.hpp>
#include "IREEAdapter.hpp"

namespace NES
{

IREEInferenceOperatorHandler::IREEInferenceOperatorHandler(Nebuli::Inference::Model model) : model(std::move(model))
{
}

void IREEInferenceOperatorHandler::start(PipelineExecutionContext& pec, uint32_t)
{
    threadLocalAdapters.reserve(pec.getNumberOfWorkerThreads());
    for (size_t threadId = 0; threadId < pec.getNumberOfWorkerThreads(); ++threadId)
    {
        threadLocalAdapters.emplace_back(IREEAdapter::create());
        threadLocalAdapters.back()->initializeModel(model);
    }
}
void IREEInferenceOperatorHandler::stop(QueryTerminationType, PipelineExecutionContext&)
{
}

const Nebuli::Inference::Model& IREEInferenceOperatorHandler::getModel() const
{
    return model;
}

const std::shared_ptr<IREEAdapter>& IREEInferenceOperatorHandler::getIREEAdapter(WorkerThreadId threadId) const
{
    return threadLocalAdapters[threadId % threadLocalAdapters.size()];
}

}
