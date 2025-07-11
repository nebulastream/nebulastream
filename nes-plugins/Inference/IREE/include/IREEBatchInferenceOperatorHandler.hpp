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

#pragma once

#include <Identifiers/Identifiers.hpp>
#include <Nautilus/Interface/PagedVector/PagedVector.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Model.hpp>
#include <WindowBasedOperatorHandler.hpp>

namespace NES
{

class IREEAdapter;
class Batch;

class IREEBatchInferenceOperatorHandler final : public WindowBasedOperatorHandler
{
public:
    IREEBatchInferenceOperatorHandler(
        const std::vector<OriginId>& inputOrigins,
        OriginId outputOriginId,
        Nebuli::Inference::Model model);

    void start(PipelineExecutionContext& pipelineExecutionContext, uint32_t localStateVariableId) override;
    void stop(QueryTerminationType terminationType, PipelineExecutionContext& pipelineExecutionContext) override;

    [[nodiscard]] const Nebuli::Inference::Model& getModel() const;
    [[nodiscard]] const std::shared_ptr<IREEAdapter>& getIREEAdapter(WorkerThreadId threadId) const;
    [[nodiscard]] Batch* getOrCreateNewBatch() const;
    void emitBatchesToProbe(Batch& batch, const BufferMetaData& bufferMetadata, PipelineExecutionContext* pipelineCtx) const;

    std::function<std::vector<std::shared_ptr<Slice>>(SliceStart, SliceEnd)> getCreateNewSlicesFunction() const override;
    void triggerSlices(
        const std::map<WindowInfoAndSequenceNumber, std::vector<std::shared_ptr<Slice>>>& ,
        PipelineExecutionContext*) override { /*noop*/ };

    uint64_t batchSize;
    mutable uint64_t tuplesSeen = 0;
    mutable std::vector<std::shared_ptr<Batch>> batches;

private:
    Nebuli::Inference::Model model;
    std::vector<std::shared_ptr<IREEAdapter>> threadLocalAdapters;
};

class Batch
{
public:
    Batch(uint64_t batchId, uint64_t numberOfWorkerThreads) : batchId(batchId)
    {
        for (uint64_t i = 0; i < numberOfWorkerThreads; ++i)
        {
            pagedVectors.emplace_back(std::make_unique<Nautilus::Interface::PagedVector>());
        }
    }

    [[nodiscard]] Nautilus::Interface::PagedVector* getPagedVectorRef(WorkerThreadId workerThreadId) const
    {
        const auto pos = workerThreadId % pagedVectors.size();
        return pagedVectors[pos].get();
    }

    void combinePagedVectors()
    {
        const std::scoped_lock lock(combinePagedVectorsMutex);
        if (pagedVectors.size() > 1)
        {
            for (uint64_t i = 1; i < pagedVectors.size(); ++i)
            {
                pagedVectors[0]->moveAllPages(*pagedVectors[i]);
            }
            pagedVectors.erase(pagedVectors.begin() + 1, pagedVectors.end());
        }
    }

    uint64_t getNumberOfTuples() const
    {
        return std::accumulate(
                pagedVectors.begin(),
                pagedVectors.end(),
                0,
                [](uint64_t sum, const auto& pagedVector) { return sum + pagedVector->getTotalNumberOfEntries(); });
    }

    uint64_t batchId;
private:
    std::vector<std::unique_ptr<Nautilus::Interface::PagedVector>> pagedVectors;
    std::mutex combinePagedVectorsMutex;
};

struct EmittedBatch
{
    uint64_t batchId;
};

}
