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

    createNewBatch();
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
    batch.state.store(BatchState::MARKED_AS_EMITTED, std::memory_order_release);

    NES_DEBUG(
        "Emitted batch {} with watermarkTs {} sequenceNumber {} originId {} tuples {}",
        bufferMemory->batchId,
        tupleBuffer.getWatermark(),
        tupleBuffer.getSequenceDataAsString(),
        tupleBuffer.getOriginId(),
        tupleBuffer.getNumberOfTuples());
}

void IREEBatchInferenceOperatorHandler::createNewBatch() const
{
    tuplesSeen = 0;
    auto batch = std::make_shared<Batch>(batches.size(), 1);
    batch->state.store(BatchState::MARKED_AS_CREATED, std::memory_order_release);
    batches.emplace_back(batch);
}

void IREEBatchInferenceOperatorHandler::garbageCollectBatches() const
{
    const std::scoped_lock lock(batchesMutex);
    if (batches.back()->state.load(std::memory_order_acquire) == BatchState::MARKED_AS_PROCESSED)
    {
        auto processedBatches = batches
            | std::views::filter([](const std::shared_ptr<Batch>& batch)
                {
                    return batch && batch->state.load() == BatchState::MARKED_AS_PROCESSED;
                });
        auto batchesCount = static_cast<size_t>(std::ranges::distance(processedBatches));
        if (batchesCount == batches.size()) batches.clear();
        createNewBatch();
        NES_DEBUG("Cleared {} batches", batchesCount);
    }
}


Batch* IREEBatchInferenceOperatorHandler::getOrCreateNewBatch() const
{
    tuplesSeen++;
    if (tuplesSeen == batchSize || batches.back()->state.load(std::memory_order_acquire) == BatchState::MARKED_AS_EMITTED)
    {
        createNewBatch();
        return batches.back().get();
    }

    return batches.back().get();
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
