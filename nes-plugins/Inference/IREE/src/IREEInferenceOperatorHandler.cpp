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

#include <IREEAdapter.hpp>
#include <IREEInferenceOperatorHandler.hpp>
#include <PipelineExecutionContext.hpp>
#include <PredictionCache/PredictionCache.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES
{

IREEInferenceOperatorHandler::IREEInferenceOperatorHandler(Nebuli::Inference::Model model) : model(std::move(model))
{
}

void IREEInferenceOperatorHandler::start(PipelineExecutionContext& pipelineExecutionContext, uint32_t)
{
    threadLocalAdapters.reserve(pipelineExecutionContext.getNumberOfWorkerThreads());
    for (size_t threadId = 0; threadId < pipelineExecutionContext.getNumberOfWorkerThreads(); ++threadId)
    {
        threadLocalAdapters.emplace_back(IREEAdapter::create());
        threadLocalAdapters.back()->initializeModel(model);
    }
}
void IREEInferenceOperatorHandler::stop(QueryTerminationType, PipelineExecutionContext& pipelineExecutionContext)
{
    const uint64_t hits = this->getHits();
    const uint64_t misses = this->getMisses();
    if (model.getInputs()[0].isType(DataType::Type::VARSIZED))
        NES_INFO("{{\"pipeline_id\": {}, \"hits\": {}, \"misses\": {}}}", pipelineExecutionContext.getPipelineId(), hits, misses)
}

const Nebuli::Inference::Model& IREEInferenceOperatorHandler::getModel() const
{
    return model;
}

const std::shared_ptr<IREEAdapter>& IREEInferenceOperatorHandler::getIREEAdapter(WorkerThreadId threadId) const
{
    return threadLocalAdapters[threadId % threadLocalAdapters.size()];
}

void IREEInferenceOperatorHandler::allocatePredictionCacheEntries(
    const uint64_t sizeOfEntry, const uint64_t numberOfEntries, AbstractBufferProvider* bufferProvider)
{
    if (hasPredictionCacheCreated.exchange(true))
    {
        return;
    }

    PRECONDITION(bufferProvider != nullptr, "Buffer provider should not be null");
    for (uint64_t i = 0; i < threadLocalAdapters.size(); ++i)
    {
        const auto neededSize = numberOfEntries * sizeOfEntry + sizeof(HitsAndMisses);
        INVARIANT(neededSize > 0, "Size of entry should be larger than 0");

        auto bufferOpt = bufferProvider->getUnpooledBuffer(neededSize);
        INVARIANT(bufferOpt.has_value(), "Buffer provider should return a buffer");
        std::memset(bufferOpt.value().getBuffer(), 0, bufferOpt.value().getBufferSize());
        predictionCacheEntriesBufferForWorkerThreads.emplace_back(bufferOpt.value());
    }
}

const int8_t* IREEInferenceOperatorHandler::getStartOfPredictionCacheEntries(const StartPredictionCacheEntriesArgs& startPredictionCacheEntriesArgs) const
{
    PRECONDITION(threadLocalAdapters.size() > 0, "Number of worker threads should be set before calling this method");
    const auto startPredictionCacheEntriesIREE = dynamic_cast<const StartPredictionCacheEntriesIREEInference&>(startPredictionCacheEntriesArgs);
    const auto pos = startPredictionCacheEntriesIREE.workerThreadId % predictionCacheEntriesBufferForWorkerThreads.size();
    INVARIANT(
        not predictionCacheEntriesBufferForWorkerThreads.empty() and pos < predictionCacheEntriesBufferForWorkerThreads.size(),
        "Position should be smaller than the size of the predictionCacheEntriesBufferForWorkerThreads");
    return predictionCacheEntriesBufferForWorkerThreads.at(pos).getBuffer();
}

}
