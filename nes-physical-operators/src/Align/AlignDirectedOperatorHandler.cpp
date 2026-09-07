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

#include <Align/AlignDirectedOperatorHandler.hpp>

#include <utility>
#include <DataTypes/UnboundSchema.hpp>
#include <Interface/PagedVector/PagedVector.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES
{

AlignDirectedOperatorHandler::AlignDirectedOperatorHandler(
    std::shared_ptr<PagedVectorTupleLayout> pendingDrivingLayout,
    std::shared_ptr<PagedVectorTupleLayout> searchedLogLayout,
    uint64_t pageSize)
    : pendingDrivingLayout(std::move(pendingDrivingLayout)), searchedLogLayout(std::move(searchedLogLayout)), pageSize(pageSize)
{
}

namespace
{
TupleBuffer allocatePagedVectorMainBuffer(AbstractBufferProvider& bufferProvider, const PagedVectorTupleLayout& layout, uint64_t pageSize)
{
    auto buffer = bufferProvider.getUnpooledBuffer(PagedVector::getMainBufferSize());
    if (!buffer.has_value())
    {
        throw BufferAllocationFailure("No unpooled TupleBuffer available for ALIGN LE/GE's paged vector!");
    }
    PagedVector::init(buffer.value(), pageSize, getSizeInBytes(layout.getSchema()));
    return std::move(buffer).value();
}
}

void AlignDirectedOperatorHandler::ensureBuffersAllocated(PipelineExecutionContext& pipelineExecutionContext)
{
    const std::lock_guard guard(mutex);
    if (!pendingDrivingBuffer)
    {
        auto bufferProvider = pipelineExecutionContext.getBufferManager();
        pendingDrivingBuffer = allocatePagedVectorMainBuffer(*bufferProvider, *pendingDrivingLayout, pageSize);
        searchedLogBuffer = allocatePagedVectorMainBuffer(*bufferProvider, *searchedLogLayout, pageSize);
    }
}

void AlignDirectedOperatorHandler::start(PipelineExecutionContext&)
{
}

void AlignDirectedOperatorHandler::stop(QueryTerminationType, PipelineExecutionContext&)
{
}

void AlignDirectedOperatorHandler::lock()
{
    mutex.lock();
}

void AlignDirectedOperatorHandler::unlock()
{
    mutex.unlock();
}

TupleBuffer* AlignDirectedOperatorHandler::getPendingDrivingBuffer()
{
    return &pendingDrivingBuffer;
}

TupleBuffer* AlignDirectedOperatorHandler::getSearchedLogBuffer()
{
    return &searchedLogBuffer;
}

void AlignDirectedOperatorHandler::setChunkNumber(
    bool isEndOfIncomingChunk, ChunkNumber incomingChunkNumber, bool isIncomingBufferTheLastChunk, TupleBuffer& buffer)
{
    emitBookkeeping.setChunkNumber(isEndOfIncomingChunk, incomingChunkNumber, isIncomingBufferTheLastChunk, buffer);
}

uint64_t AlignDirectedOperatorHandler::getPendingDrivingFrontIndex() const
{
    return pendingDrivingFrontIndex;
}

void AlignDirectedOperatorHandler::setPendingDrivingFrontIndex(uint64_t index)
{
    pendingDrivingFrontIndex = index;
}

uint64_t AlignDirectedOperatorHandler::getSearchedLogFrontIndex() const
{
    return searchedLogFrontIndex;
}

void AlignDirectedOperatorHandler::setSearchedLogFrontIndex(uint64_t index)
{
    searchedLogFrontIndex = index;
}

}
