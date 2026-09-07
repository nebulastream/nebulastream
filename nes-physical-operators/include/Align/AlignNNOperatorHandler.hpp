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

class AlignNNOperatorHandler final : public OperatorHandler
{
public:
    AlignNNOperatorHandler(
        std::shared_ptr<PagedVectorTupleLayout> pendingLeftLayout,
        std::shared_ptr<PagedVectorTupleLayout> rightLogLayout,
        uint64_t pageSize);

    void start(PipelineExecutionContext& pipelineExecutionContext) override;
    void stop(QueryTerminationType terminationType, PipelineExecutionContext& pipelineExecutionContext) override;

    void ensureBuffersAllocated(PipelineExecutionContext& pipelineExecutionContext);

    void lock();
    void unlock();

    [[nodiscard]] TupleBuffer* getPendingLeftBuffer();
    [[nodiscard]] TupleBuffer* getRightLogBuffer();
    [[nodiscard]] uint64_t getPendingLeftFrontIndex() const;
    void setPendingLeftFrontIndex(uint64_t index);
    [[nodiscard]] uint64_t getRightLogFrontIndex() const;
    void setRightLogFrontIndex(uint64_t index);

    void setChunkNumber(bool isEndOfIncomingChunk, ChunkNumber incomingChunkNumber, bool isIncomingBufferTheLastChunk, TupleBuffer& buffer);

    const std::shared_ptr<PagedVectorTupleLayout> pendingLeftLayout;
    const std::shared_ptr<PagedVectorTupleLayout> rightLogLayout;

private:
    const uint64_t pageSize;
    TupleBuffer pendingLeftBuffer;
    TupleBuffer rightLogBuffer;
    uint64_t pendingLeftFrontIndex = 0;
    uint64_t rightLogFrontIndex = 0;
    std::mutex mutex;
    EmitOperatorHandler emitBookkeeping;
};

}
