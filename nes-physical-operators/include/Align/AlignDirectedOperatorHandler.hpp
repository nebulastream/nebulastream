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

#include <cstdint>
#include <memory>
#include <mutex>
#include <EmitOperatorHandler.hpp>
#include <Interface/PagedVector/PagedVectorRef.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <Runtime/TupleBuffer.hpp>

namespace NES
{
class PipelineExecutionContext;

class AlignDirectedOperatorHandler final : public OperatorHandler
{
public:
    AlignDirectedOperatorHandler(
        std::shared_ptr<PagedVectorTupleLayout> pendingDrivingLayout,
        std::shared_ptr<PagedVectorTupleLayout> searchedLogLayout,
        uint64_t pageSize);

    void start(PipelineExecutionContext& pipelineExecutionContext) override;
    void stop(QueryTerminationType terminationType, PipelineExecutionContext& pipelineExecutionContext) override;

    void ensureBuffersAllocated(PipelineExecutionContext& pipelineExecutionContext);

    void lock();
    void unlock();

    [[nodiscard]] TupleBuffer* getPendingDrivingBuffer();
    [[nodiscard]] TupleBuffer* getSearchedLogBuffer();
    [[nodiscard]] uint64_t getPendingDrivingFrontIndex() const;
    void setPendingDrivingFrontIndex(uint64_t index);
    [[nodiscard]] uint64_t getSearchedLogFrontIndex() const;
    void setSearchedLogFrontIndex(uint64_t index);

    void setChunkNumber(bool isEndOfIncomingChunk, ChunkNumber incomingChunkNumber, bool isIncomingBufferTheLastChunk, TupleBuffer& buffer);

    const std::shared_ptr<PagedVectorTupleLayout> pendingDrivingLayout;
    const std::shared_ptr<PagedVectorTupleLayout> searchedLogLayout;

private:
    const uint64_t pageSize;
    TupleBuffer pendingDrivingBuffer;
    TupleBuffer searchedLogBuffer;
    uint64_t pendingDrivingFrontIndex = 0;
    uint64_t searchedLogFrontIndex = 0;
    std::mutex mutex;
    EmitOperatorHandler emitBookkeeping;
};

}
