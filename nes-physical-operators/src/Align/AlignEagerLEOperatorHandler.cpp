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

#include <Align/AlignEagerLEOperatorHandler.hpp>

#include <utility>
#include <Runtime/AbstractBufferProvider.hpp>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES
{

AlignEagerLEOperatorHandler::AlignEagerLEOperatorHandler(std::shared_ptr<TupleBufferRef> searchedStateLayout, uint64_t bufferSize)
    : searchedStateLayout(std::move(searchedStateLayout)), bufferSize(bufferSize)
{
}

void AlignEagerLEOperatorHandler::ensureBufferAllocated(PipelineExecutionContext& pipelineExecutionContext)
{
    const std::lock_guard guard(mutex);
    if (!searchedStateBuffer)
    {
        auto bufferProvider = pipelineExecutionContext.getBufferManager();
        auto buffer = bufferProvider->getUnpooledBuffer(bufferSize);
        if (!buffer.has_value())
        {
            throw BufferAllocationFailure("No unpooled TupleBuffer available for ALIGN eager LE's state slot!");
        }
        searchedStateBuffer = std::move(buffer).value();
    }
}

void AlignEagerLEOperatorHandler::start(PipelineExecutionContext&)
{
}

void AlignEagerLEOperatorHandler::stop(QueryTerminationType, PipelineExecutionContext&)
{
}

void AlignEagerLEOperatorHandler::lock()
{
    mutex.lock();
}

void AlignEagerLEOperatorHandler::unlock()
{
    mutex.unlock();
}

TupleBuffer* AlignEagerLEOperatorHandler::getSearchedStateBuffer()
{
    return &searchedStateBuffer;
}

bool AlignEagerLEOperatorHandler::hasSearchedRecord() const
{
    return hasRecord;
}

void AlignEagerLEOperatorHandler::setHasSearchedRecord(bool value)
{
    hasRecord = value;
}

void AlignEagerLEOperatorHandler::setChunkNumber(
    bool isEndOfIncomingChunk, ChunkNumber incomingChunkNumber, bool isIncomingBufferTheLastChunk, TupleBuffer& buffer)
{
    emitBookkeeping.setChunkNumber(isEndOfIncomingChunk, incomingChunkNumber, isIncomingBufferTheLastChunk, buffer);
}

}
