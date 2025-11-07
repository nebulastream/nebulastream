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
#include <PredictionCacheOperatorHandler.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Model.hpp>

namespace NES
{
class IREEAdapter;
class IREEInferenceOperatorHandler : public OperatorHandler, public PredictionCacheOperatorHandler
{
public:
    IREEInferenceOperatorHandler(Nebuli::Inference::Model model);

    void start(PipelineExecutionContext& pipelineExecutionContext, uint32_t localStateVariableId) override;
    void stop(QueryTerminationType terminationType, PipelineExecutionContext& pipelineExecutionContext) override;

    [[nodiscard]] const Nebuli::Inference::Model& getModel() const;
    [[nodiscard]] const std::shared_ptr<IREEAdapter>& getIREEAdapter(WorkerThreadId threadId) const;
    void allocatePredictionCacheEntries(
        const uint64_t sizeOfEntry, const uint64_t numberOfEntries, AbstractBufferProvider* bufferProvider) override;

    struct StartPredictionCacheEntriesIREEInference final : StartPredictionCacheEntriesArgs
    {
        explicit StartPredictionCacheEntriesIREEInference(const WorkerThreadId workerThreadId)
            : StartPredictionCacheEntriesArgs(workerThreadId)
        {
        }

        StartPredictionCacheEntriesIREEInference(StartPredictionCacheEntriesIREEInference&& other) = default;
        StartPredictionCacheEntriesIREEInference& operator=(StartPredictionCacheEntriesIREEInference&& other) = default;

        StartPredictionCacheEntriesIREEInference(const StartPredictionCacheEntriesIREEInference& other)
            : StartPredictionCacheEntriesArgs(other.workerThreadId)
        {
        }

        StartPredictionCacheEntriesIREEInference& operator=(const StartPredictionCacheEntriesIREEInference& other)
        {
            workerThreadId = other.workerThreadId;
            return *this;
        };

        ~StartPredictionCacheEntriesIREEInference() override = default;
    };

    const int8_t* getStartOfPredictionCacheEntries(const StartPredictionCacheEntriesArgs& startPredictionCacheEntriesArgs) const override;

    uint64_t getHits() { return hits.load(std::memory_order_relaxed); }
    uint64_t getMisses() { return misses.load(std::memory_order_relaxed); }
    void incrementHits(uint64_t currentHits) const noexcept { hits += currentHits; }
    void incrementMisses(uint64_t currentMisses) const noexcept { misses += currentMisses; }

private:
    Nebuli::Inference::Model model;
    std::vector<std::shared_ptr<IREEAdapter>> threadLocalAdapters;
    mutable std::atomic<uint64_t> hits{0};
    mutable std::atomic<uint64_t> misses{0};
};

}