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

#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>

namespace NES
{

class PredictionCacheOperatorHandler
{
public:
    virtual ~PredictionCacheOperatorHandler() = default;
    virtual void
    allocatePredictionCacheEntries(const uint64_t sizeOfEntry, const uint64_t numberOfEntries, AbstractBufferProvider* bufferProvider)
        = 0;

    struct StartPredictionCacheEntriesArgs
    {
        WorkerThreadId workerThreadId;

        explicit StartPredictionCacheEntriesArgs(const WorkerThreadId& workerThreadId) : workerThreadId(workerThreadId) { }

        StartPredictionCacheEntriesArgs(StartPredictionCacheEntriesArgs&& other) = default;
        StartPredictionCacheEntriesArgs& operator=(StartPredictionCacheEntriesArgs&& other) = default;
        StartPredictionCacheEntriesArgs(StartPredictionCacheEntriesArgs& other) = default;
        StartPredictionCacheEntriesArgs& operator=(StartPredictionCacheEntriesArgs& other) = default;
        virtual ~StartPredictionCacheEntriesArgs() = default;
    };

    virtual const int8_t* getStartOfPredictionCacheEntries(const StartPredictionCacheEntriesArgs& startPredictionCacheEntriesArgs) const = 0;

protected:
    std::vector<TupleBuffer> predictionCacheEntriesBufferForWorkerThreads;
    std::atomic<bool> hasPredictionCacheCreated{false};
};

}
