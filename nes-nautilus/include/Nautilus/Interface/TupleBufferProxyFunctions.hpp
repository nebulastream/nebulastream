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
#include <Identifiers/Identifiers.hpp>
#include <Runtime/PinnedBuffer.hpp>
#include <Time/Timestamp.hpp>

namespace NES::Nautilus::ProxyFunctions
{
inline int8_t* NES_Memory_TupleBuffer_getBuffer(Memory::PinnedBuffer* tupleBuffer)
{
    return tupleBuffer->getBuffer();
};

inline uint64_t NES_Memory_TupleBuffer_getBufferSize(const Memory::PinnedBuffer* tupleBuffer)
{
    return tupleBuffer->getBufferSize();
};

inline uint64_t NES_Memory_TupleBuffer_getNumberOfTuples(const Memory::PinnedBuffer* tupleBuffer)
{
    return tupleBuffer->getNumberOfTuples();
};

void inline NES_Memory_TupleBuffer_setNumberOfTuples(Memory::PinnedBuffer* tupleBuffer, const uint64_t numberOfTuples)
{
    tupleBuffer->setNumberOfTuples(numberOfTuples);
}

inline OriginId NES_Memory_TupleBuffer_getOriginId(const Memory::PinnedBuffer* tupleBuffer)
{
    return tupleBuffer->getOriginId();
};

inline void NES_Memory_TupleBuffer_setOriginId(Memory::PinnedBuffer* tupleBuffer, const OriginId value)
{
    tupleBuffer->setOriginId(OriginId(value));
};

inline Runtime::Timestamp NES_Memory_TupleBuffer_getWatermark(const Memory::PinnedBuffer* tupleBuffer)
{
    return tupleBuffer->getWatermark();
};

inline void NES_Memory_TupleBuffer_setWatermark(Memory::PinnedBuffer* tupleBuffer, const Runtime::Timestamp value)
{
    tupleBuffer->setWatermark(Runtime::Timestamp(value));
};

inline Runtime::Timestamp NES_Memory_TupleBuffer_getCreationTimestampInMS(const Memory::PinnedBuffer* tupleBuffer)
{
    return tupleBuffer->getCreationTimestampInMS();
};

inline void NES_Memory_TupleBuffer_setSequenceNumber(Memory::PinnedBuffer* tupleBuffer, const SequenceNumber sequenceNumber)
{
    tupleBuffer->setSequenceNumber(sequenceNumber);
};

inline SequenceNumber NES_Memory_TupleBuffer_getSequenceNumber(const Memory::PinnedBuffer* tupleBuffer)
{
    return tupleBuffer->getSequenceNumber();
}

inline void NES_Memory_TupleBuffer_setCreationTimestampInMS(Memory::PinnedBuffer* tupleBuffer, const Runtime::Timestamp value)
{
    tupleBuffer->setCreationTimestampInMS(Runtime::Timestamp(value));
}

inline void NES_Memory_TupleBuffer_setChunkNumber(Memory::PinnedBuffer* tupleBuffer, const ChunkNumber chunkNumber)
{
    tupleBuffer->setChunkNumber(ChunkNumber(chunkNumber));
};

inline void NES_Memory_TupleBuffer_setLastChunk(Memory::PinnedBuffer* tupleBuffer, const bool isLastChunk)
{
    tupleBuffer->setLastChunk(isLastChunk);
};

inline ChunkNumber NES_Memory_TupleBuffer_getChunkNumber(const Memory::PinnedBuffer* tupleBuffer)
{
    return tupleBuffer->getChunkNumber();
};

inline bool NES_Memory_TupleBuffer_isLastChunk(const Memory::PinnedBuffer* tupleBuffer)
{
    return tupleBuffer->isLastChunk();
};

}
