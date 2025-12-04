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
#include <IREEBatchInferenceOperatorHandler.hpp>
#include <Util/Logger/Logger.hpp>
#include <PipelineExecutionContext.hpp>
#include <PredictionCache.hpp>
#include <WindowBasedOperatorHandler.hpp>

namespace NES
{

IREEBatchInferenceOperatorHandler::IREEBatchInferenceOperatorHandler(
    const std::vector<OriginId>& inputOrigins,
    OriginId outputOriginId,
    Nebuli::Inference::Model model,
    uint64_t batchSize)
    : WindowBasedOperatorHandler(inputOrigins, outputOriginId, true)
    , model(std::move(model))
    , batchSize(batchSize)
{
}

void IREEBatchInferenceOperatorHandler::start(PipelineExecutionContext& pipelineExecutionContext, uint32_t)
{
    numberOfWorkerThreads = pipelineExecutionContext.getNumberOfWorkerThreads();
    watermarkProcessorBuild = std::make_unique<MultiOriginWatermarkProcessor>(inputOrigins);
    watermarkProcessorProbe = std::make_unique<MultiOriginWatermarkProcessor>(std::vector{outputOriginId});

    threadLocalAdapters.reserve(numberOfWorkerThreads);
    for (size_t threadId = 0; threadId < numberOfWorkerThreads; ++threadId)
    {
        threadLocalAdapters.emplace_back(IREEAdapter::create());
        threadLocalAdapters.back()->initializeModel(model);
    }
}

void IREEBatchInferenceOperatorHandler::stop(QueryTerminationType, PipelineExecutionContext&)
{
    threadLocalAdapters.clear();
}

const Nebuli::Inference::Model& IREEBatchInferenceOperatorHandler::getModel() const
{
    return model;
}

const std::shared_ptr<IREEAdapter>& IREEBatchInferenceOperatorHandler::getIREEAdapter(WorkerThreadId workerThreadId) const
{
    return threadLocalAdapters[workerThreadId % threadLocalAdapters.size()];
}

void IREEBatchInferenceOperatorHandler::emitBatchesToProbe(
    Batch& batch,
    const SequenceData& sequenceData,
    PipelineExecutionContext* pipelineCtx,
    const Timestamp watermarkTs) const
{
    batch.combinePagedVectors();
    const auto numberOfTuples = batch.getNumberOfTuples();

    auto tupleBuffer = pipelineCtx->getBufferManager()->getBufferBlocking();
    tupleBuffer.setOriginId(outputOriginId);
    tupleBuffer.setSequenceNumber(SequenceNumber(sequenceData.sequenceNumber));
    tupleBuffer.setChunkNumber(ChunkNumber(sequenceData.chunkNumber));
    tupleBuffer.setLastChunk(sequenceData.lastChunk);
    tupleBuffer.setWatermark(watermarkTs);
    tupleBuffer.setNumberOfTuples(numberOfTuples);

    auto bufferMemory = tupleBuffer.getAvailableMemoryArea();
    new (bufferMemory.data()) EmittedBatch{batch.batchId};

    pipelineCtx->emitBuffer(tupleBuffer);
    batch.setState(BatchState::MARKED_AS_EMITTED);

    NES_TRACE(
        "Emitted batch {} with watermarkTs {} sequenceNumber {} originId {} tuples {}",
        batch.batchId,
        tupleBuffer.getWatermark(),
        tupleBuffer.getSequenceDataAsString(),
        tupleBuffer.getOriginId(),
        tupleBuffer.getNumberOfTuples());
}

std::shared_ptr<Batch> IREEBatchInferenceOperatorHandler::createNewBatch() const
{
    tuplesSeen = 0;
    ++batchId;
    auto batch = std::make_shared<Batch>(batchId, 1);
    batch->setState(BatchState::MARKED_AS_CREATED);
    return batch;
}

std::shared_ptr<Batch> IREEBatchInferenceOperatorHandler::getBatch(uint64_t batchId) const
{
    return batches.rlock()->at(batchId);
}

Batch* IREEBatchInferenceOperatorHandler::getOrCreateNewBatch() const
{
    auto batchesWriteLock = batches.wlock();
    tuplesSeen++;
    if (tuplesSeen == batchSize || !batchesWriteLock->contains(batchId) ||
        (batchesWriteLock->contains(batchId) && batchesWriteLock->at(batchId)->state == BatchState::MARKED_AS_EMITTED))
    {
        std::shared_ptr<Batch> batch = createNewBatch();
        batchesWriteLock->insert(std::make_pair(batchId, batch));
        return batch.get();
    }

    return batchesWriteLock->at(batchId).get();
}

void IREEBatchInferenceOperatorHandler::garbageCollectBatches() const
{
    auto batchesWriteLock = batches.wlock();

    if (batchesWriteLock->contains(batchId) && batchesWriteLock->at(batchId)->state == BatchState::MARKED_AS_PROCESSED)
    {
        auto processedBatches = *batchesWriteLock
            | std::views::filter([](const auto& pair)
                {
                    const auto& batch = pair.second;
                    return batch && batch->state == BatchState::MARKED_AS_PROCESSED;
                })
            | std::views::transform([](const auto& pair)
                {
                    return pair.first;
                });
        auto batchesCount = static_cast<size_t>(std::ranges::distance(processedBatches));

        std::vector batchesToErase(processedBatches.begin(), processedBatches.end());
        for (const uint64_t batchId : batchesToErase)
        {
            batchesWriteLock->erase(batchId);
        }
    }
}

void IREEBatchInferenceOperatorHandler::allocatePredictionCacheEntries(
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
        std::ranges::fill(bufferOpt.value().getAvailableMemoryArea(), std::byte{0});
        predictionCacheEntriesBufferForWorkerThreads.emplace_back(bufferOpt.value());
    }
}

const int8_t* IREEBatchInferenceOperatorHandler::getStartOfPredictionCacheEntries(const StartPredictionCacheEntriesArgs& startPredictionCacheEntriesArgs) const
{
    PRECONDITION(threadLocalAdapters.size() > 0, "Number of worker threads should be set before calling this method");
    const auto startPredictionCacheEntriesIREE = dynamic_cast<const StartPredictionCacheEntriesIREEInference&>(startPredictionCacheEntriesArgs);
    const auto pos = startPredictionCacheEntriesIREE.workerThreadId % predictionCacheEntriesBufferForWorkerThreads.size();
    INVARIANT(
        not predictionCacheEntriesBufferForWorkerThreads.empty() and pos < predictionCacheEntriesBufferForWorkerThreads.size(),
        "Position should be smaller than the size of the predictionCacheEntriesBufferForWorkerThreads");
    return predictionCacheEntriesBufferForWorkerThreads.at(pos).getAvailableMemoryArea<int8_t>().data();
}

std::function<std::vector<std::shared_ptr<Slice>>(SliceStart, SliceEnd)>
IREEBatchInferenceOperatorHandler::getCreateNewSlicesFunction(const CreateNewSlicesArguments&) const
{
    return [](SliceStart, SliceEnd)
    {
        return std::vector<std::shared_ptr<Slice>>{};
    };
}

}
