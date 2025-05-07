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

#include <SequenceOperatorHandler.hpp>

#include <cstdint>
#include <memory>
#include <optional>
#include <utility>
#include <Runtime/TupleBuffer.hpp>
#include <ErrorHandling.hpp>

namespace NES::Runtime::Execution::Operators
{

std::optional<Memory::TupleBuffer*> SequenceOperatorHandler::getNextBuffer(uint8_t* bufferMemory)
{
    auto buffer = Memory::TupleBuffer::reinterpretAsTupleBuffer(bufferMemory);
    if (auto optBuffer = sequencer.isNext(SequenceData(buffer.getSequenceNumber(), buffer.getChunkNumber(), buffer.isLastChunk()), buffer))
    {
        currentBuffer = std::move(*optBuffer);
        return std::addressof(currentBuffer);
    }
    return {};
}

std::optional<Memory::TupleBuffer*> SequenceOperatorHandler::markBufferAsDone(Memory::TupleBuffer* buffer)
{
    INVARIANT(buffer == std::addressof(currentBuffer), "Not sure where this pointer is comming from");
    auto optNextBuffer
        = sequencer.advanceAndGetNext(SequenceData(buffer->getSequenceNumber(), buffer->getChunkNumber(), buffer->isLastChunk()));
    if (optNextBuffer)
    {
        currentBuffer = std::move(*optNextBuffer);
        return std::addressof(currentBuffer);
    }
    return {};
}
}
