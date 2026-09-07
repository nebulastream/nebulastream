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
#include <Interface/BufferRef/TupleBufferRef.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <Runtime/TupleBuffer.hpp>

namespace NES
{
class PipelineExecutionContext;

class AlignEagerLEOperatorHandler final : public OperatorHandler
{
public:
    AlignEagerLEOperatorHandler(std::shared_ptr<TupleBufferRef> searchedStateLayout, uint64_t bufferSize);

    void start(PipelineExecutionContext& pipelineExecutionContext) override;
    void stop(QueryTerminationType terminationType, PipelineExecutionContext& pipelineExecutionContext) override;

    void ensureBufferAllocated(PipelineExecutionContext& pipelineExecutionContext);

    void lock();
    void unlock();

    [[nodiscard]] TupleBuffer* getSearchedStateBuffer();
    [[nodiscard]] bool hasSearchedRecord() const;
    void setHasSearchedRecord(bool value);

    void setChunkNumber(bool isEndOfIncomingChunk, ChunkNumber incomingChunkNumber, bool isIncomingBufferTheLastChunk, TupleBuffer& buffer);

    const std::shared_ptr<TupleBufferRef> searchedStateLayout;

private:
    const uint64_t bufferSize;
    TupleBuffer searchedStateBuffer;
    bool hasRecord = false;
    std::mutex mutex;
    EmitOperatorHandler emitBookkeeping;
};

}
