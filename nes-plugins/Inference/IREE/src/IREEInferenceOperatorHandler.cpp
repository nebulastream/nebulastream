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
#include <WindowBasedOperatorHandler.hpp>

namespace NES
{

IREEInferenceOperatorHandler::IREEInferenceOperatorHandler(
    const std::vector<OriginId>& inputOrigins,
    OriginId outputOriginId,
    Nebuli::Inference::Model model)
    : WindowBasedOperatorHandler(inputOrigins, outputOriginId)
    , batchSize(model.getInputShape().front())
    , model(std::move(model))
{
}

void IREEInferenceOperatorHandler::start(PipelineExecutionContext& pipelineExecutionContext, uint32_t)
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

void IREEInferenceOperatorHandler::stop(QueryTerminationType, PipelineExecutionContext&)
{
}

const Nebuli::Inference::Model& IREEInferenceOperatorHandler::getModel() const
{
    return model;
}

const std::shared_ptr<IREEAdapter>& IREEInferenceOperatorHandler::getIREEAdapter(WorkerThreadId workerThreadId) const
{
    return threadLocalAdapters[workerThreadId % threadLocalAdapters.size()];
}

void IREEInferenceOperatorHandler::emitBatchesToProbe(
    Batch& batch,
    const BufferMetaData& bufferMetadata,
    PipelineExecutionContext* pipelineCtx) const
{
    auto tupleBuffer = pipelineCtx->getBufferManager()->getBufferBlocking();
    const auto sequenceData = bufferMetadata.seqNumber;

    tupleBuffer.setOriginId(outputOriginId);
    tupleBuffer.setSequenceNumber(SequenceNumber(sequenceData.sequenceNumber));
    tupleBuffer.setChunkNumber(ChunkNumber(sequenceData.chunkNumber));
    tupleBuffer.setLastChunk(sequenceData.lastChunk);
    tupleBuffer.setWatermark(bufferMetadata.watermarkTs);
    tupleBuffer.setNumberOfTuples(batchSize);

    auto* bufferMemory = tupleBuffer.getBuffer<EmittedBatch>();
    bufferMemory->batchId = batch.batchId;

    pipelineCtx->emitBuffer(tupleBuffer);
}

Batch* IREEInferenceOperatorHandler::getOrCreateNewBatch() const
{
    tuplesSeen++;
    if (batches.empty() || tuplesSeen == batchSize)
    {
        tuplesSeen = 0;
        auto batch = std::make_shared<Batch>(batches.size(), numberOfWorkerThreads);
        batches.emplace_back(batch);
        return batch.get();
    }
    return batches.back().get();
}

std::function<std::vector<std::shared_ptr<Slice>>(SliceStart, SliceEnd)> IREEInferenceOperatorHandler::getCreateNewSlicesFunction() const
{
    return [](SliceStart, SliceEnd)
    {
        return std::vector<std::shared_ptr<Slice>>{};
    };
}

}
