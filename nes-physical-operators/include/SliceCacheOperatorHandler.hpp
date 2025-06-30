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

namespace NES
{
class SliceCacheOperatorHandler
{
public:
    virtual ~SliceCacheOperatorHandler() = default;
    virtual void
    allocateSliceCacheEntries(const uint64_t sizeOfEntry, const uint64_t numberOfEntries, Memory::AbstractBufferProvider* bufferProvider)
        = 0;

    /// This is the parent struct so that we can define an interface method for how to acquire the start of the slice cache entries
    /// Each operator handler that implements this interface should inherit from this struct and can then add their needed members.
    struct StartSliceCacheEntriesArgs
    {
        WorkerThreadId workerThreadId;

        explicit StartSliceCacheEntriesArgs(const WorkerThreadId& workerThreadId) : workerThreadId(workerThreadId) { }
        StartSliceCacheEntriesArgs(StartSliceCacheEntriesArgs&& other) = default;
        StartSliceCacheEntriesArgs& operator=(StartSliceCacheEntriesArgs&& other) = default;
        StartSliceCacheEntriesArgs(StartSliceCacheEntriesArgs& other) = default;
        StartSliceCacheEntriesArgs& operator=(StartSliceCacheEntriesArgs& other) = default;
        virtual ~StartSliceCacheEntriesArgs() = default;
    };
    virtual const int8_t* getStartOfSliceCacheEntries(const StartSliceCacheEntriesArgs& startSliceCacheEntriesArgs) const = 0;

protected:
    /// Might not be the best way to do this, but it is fine for this PoC branch.
    /// This vector stores the start of the cache entries for each worker thread.
    std::vector<Memory::TupleBuffer> sliceCacheEntriesBufferForWorkerThreads;
    std::atomic<bool> hasSliceCacheCreated{false};
};

}
