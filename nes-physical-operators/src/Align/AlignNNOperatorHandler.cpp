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

#include <Align/AlignNNOperatorHandler.hpp>

#include <utility>
#include <DataTypes/UnboundSchema.hpp>
#include <Interface/PagedVector/PagedVector.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES
{

AlignNNOperatorHandler::AlignNNOperatorHandler(
    std::shared_ptr<PagedVectorTupleLayout> pendingLeftLayout, std::shared_ptr<PagedVectorTupleLayout> rightLogLayout, uint64_t pageSize)
    : pendingLeftLayout(std::move(pendingLeftLayout)), rightLogLayout(std::move(rightLogLayout)), pageSize(pageSize)
{
}

namespace
{
TupleBuffer allocatePagedVectorMainBuffer(AbstractBufferProvider& bufferProvider, const PagedVectorTupleLayout& layout, uint64_t pageSize)
{
    auto buffer = bufferProvider.getUnpooledBuffer(PagedVector::getMainBufferSize());
    if (!buffer.has_value())
    {
        throw BufferAllocationFailure("No unpooled TupleBuffer available for ALIGN NN's paged vector!");
    }
    PagedVector::init(buffer.value(), pageSize, getSizeInBytes(layout.getSchema()));
    return std::move(buffer).value();
}
}

void AlignNNOperatorHandler::ensureBuffersAllocated(PipelineExecutionContext& pipelineExecutionContext)
{
    const std::lock_guard guard(mutex);
    if (!pendingLeftBuffer)
    {
        auto bufferProvider = pipelineExecutionContext.getBufferManager();
        pendingLeftBuffer = allocatePagedVectorMainBuffer(*bufferProvider, *pendingLeftLayout, pageSize);
        rightLogBuffer = allocatePagedVectorMainBuffer(*bufferProvider, *rightLogLayout, pageSize);
    }
}

void AlignNNOperatorHandler::start(PipelineExecutionContext&)
{
}

void AlignNNOperatorHandler::stop(QueryTerminationType, PipelineExecutionContext&)
{
}

void AlignNNOperatorHandler::lock()
{
    mutex.lock();
}

void AlignNNOperatorHandler::unlock()
{
    mutex.unlock();
}

TupleBuffer* AlignNNOperatorHandler::getPendingLeftBuffer()
{
    return &pendingLeftBuffer;
}

TupleBuffer* AlignNNOperatorHandler::getRightLogBuffer()
{
    return &rightLogBuffer;
}

void AlignNNOperatorHandler::setChunkNumber(
    bool isEndOfIncomingChunk, ChunkNumber incomingChunkNumber, bool isIncomingBufferTheLastChunk, TupleBuffer& buffer)
{
    emitBookkeeping.setChunkNumber(isEndOfIncomingChunk, incomingChunkNumber, isIncomingBufferTheLastChunk, buffer);
}

uint64_t AlignNNOperatorHandler::getPendingLeftFrontIndex() const
{
    return pendingLeftFrontIndex;
}

void AlignNNOperatorHandler::setPendingLeftFrontIndex(uint64_t index)
{
    pendingLeftFrontIndex = index;
}

uint64_t AlignNNOperatorHandler::getRightLogFrontIndex() const
{
    return rightLogFrontIndex;
}

void AlignNNOperatorHandler::setRightLogFrontIndex(uint64_t index)
{
    rightLogFrontIndex = index;
}

}
