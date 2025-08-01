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

#include <Util/Logger/Logger.hpp>
#include <PipelineExecutionContext.hpp>
#include <WindowBasedOperatorHandler.hpp>
#include "IREEAdapter.hpp"
#include "IREEBatchInferenceOperatorHandler.hpp"

namespace NES
{

IREEBatchInferenceOperatorHandler::IREEBatchInferenceOperatorHandler(
    const std::vector<OriginId>& inputOrigins,
    OriginId outputOriginId,
    Nebuli::Inference::Model model)
    : WindowBasedOperatorHandler(inputOrigins, outputOriginId, true)
    , batchSize(model.getInputShape().front())
    , model(std::move(model))
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

    auto* bufferMemory = tupleBuffer.getBuffer<EmittedBatch>();
    bufferMemory->batchId = batch.batchId;

    pipelineCtx->emitBuffer(tupleBuffer);
    batch.setState(BatchState::MARKED_AS_EMITTED);

    NES_DEBUG(
        "Emitted batch {} with watermarkTs {} sequenceNumber {} originId {} tuples {}",
        bufferMemory->batchId,
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
        NES_DEBUG("Top batch ID {} state {}", batchId, static_cast<uint8_t>(batchesWriteLock->at(batchId)->state))
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
        NES_DEBUG("Batches to clear {}, total {}", batchesCount, batchesWriteLock->size());

        std::vector batchesToErase(processedBatches.begin(), processedBatches.end());
        for (const uint64_t batchId : batchesToErase)
        {
            batchesWriteLock->erase(batchId);
        }
    }
}

std::function<std::vector<std::shared_ptr<Slice>>(SliceStart, SliceEnd)>
IREEBatchInferenceOperatorHandler::getCreateNewSlicesFunction() const
{
    return [](SliceStart, SliceEnd)
    {
        return std::vector<std::shared_ptr<Slice>>{};
    };
}

}
