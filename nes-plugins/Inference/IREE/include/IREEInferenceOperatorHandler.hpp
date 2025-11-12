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

    uint64_t getHits()
    {
        const uint64_t result = std::accumulate
        (
            std::begin(hits),
            std::end(hits),
            0, // initial value of the sum
            [](const std::uint64_t previous, const std::pair<const WorkerThreadId, uint32_t>& p)
            { return previous + p.second; }
        );
        return result;
    }
    uint64_t getMisses()
    {
        const uint64_t result = std::accumulate
        (
            std::begin(misses),
            std::end(misses),
            0, // initial value of the sum
            [](const std::uint64_t previous, const std::pair<const WorkerThreadId, uint32_t>& p)
            { return previous + p.second; }
        );
        return result;
    }
    void incrementHits(uint64_t currentHits, WorkerThreadId thread) const noexcept
    {
        hits[thread] += currentHits;
    }
    void incrementMisses(uint64_t currentMisses, WorkerThreadId thread) const noexcept
    {
        misses[thread] += currentMisses;
    }
    std::mutex mutex;

private:
    Nebuli::Inference::Model model;
    std::vector<std::shared_ptr<IREEAdapter>> threadLocalAdapters;
    // mutable std::atomic<uint64_t> hits{0};
    // mutable std::atomic<uint64_t> misses{0};
    mutable std::unordered_map<WorkerThreadId, uint64_t> hits;
    mutable std::unordered_map<WorkerThreadId, uint64_t> misses;
};

}