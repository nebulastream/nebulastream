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
#include <Runtime/TupleBuffer.hpp>
#include <Time/Timestamp.hpp>
#include "inline.hpp"

namespace NES::Nautilus::ProxyFunctions
{
NAUT_INLINE inline int8_t* NES_Memory_TupleBuffer_getBuffer(TupleBuffer* tupleBuffer)
{
    return tupleBuffer->getBuffer();
};

NAUT_INLINE inline uint64_t NES_Memory_TupleBuffer_getBufferSize(const TupleBuffer* tupleBuffer)
{
    return tupleBuffer->getBufferSize();
};

NAUT_INLINE inline uint64_t NES_Memory_TupleBuffer_getNumberOfTuples(const TupleBuffer* tupleBuffer)
{
    return tupleBuffer->getNumberOfTuples();
};

NAUT_INLINE void inline NES_Memory_TupleBuffer_setNumberOfTuples(TupleBuffer* tupleBuffer, const uint64_t numberOfTuples)
{
    tupleBuffer->setNumberOfTuples(numberOfTuples);
}

NAUT_INLINE inline OriginId NES_Memory_TupleBuffer_getOriginId(const TupleBuffer* tupleBuffer)
{
    return tupleBuffer->getOriginId();
};

NAUT_INLINE inline void NES_Memory_TupleBuffer_setOriginId(TupleBuffer* tupleBuffer, const OriginId value)
{
    tupleBuffer->setOriginId(OriginId(value));
};

NAUT_INLINE inline Timestamp NES_Memory_TupleBuffer_getWatermark(const TupleBuffer* tupleBuffer)
{
    return tupleBuffer->getWatermark();
};

NAUT_INLINE inline void NES_Memory_TupleBuffer_setWatermark(TupleBuffer* tupleBuffer, const Timestamp value)
{
    tupleBuffer->setWatermark(Timestamp(value));
};

NAUT_INLINE inline Timestamp NES_Memory_TupleBuffer_getCreationTimestampInMS(const TupleBuffer* tupleBuffer)
{
    return tupleBuffer->getCreationTimestampInMS();
};

NAUT_INLINE inline void NES_Memory_TupleBuffer_setSequenceNumber(TupleBuffer* tupleBuffer, const SequenceNumber sequenceNumber)
{
    tupleBuffer->setSequenceNumber(sequenceNumber);
};

NAUT_INLINE inline SequenceNumber NES_Memory_TupleBuffer_getSequenceNumber(const TupleBuffer* tupleBuffer)
{
    return tupleBuffer->getSequenceNumber();
}

NAUT_INLINE inline void NES_Memory_TupleBuffer_setCreationTimestampInMS(TupleBuffer* tupleBuffer, const Timestamp value)
{
    tupleBuffer->setCreationTimestampInMS(Timestamp(value));
}

NAUT_INLINE inline void NES_Memory_TupleBuffer_setChunkNumber(TupleBuffer* tupleBuffer, const ChunkNumber chunkNumber)
{
    tupleBuffer->setChunkNumber(ChunkNumber(chunkNumber));
};

NAUT_INLINE inline void NES_Memory_TupleBuffer_setLastChunk(TupleBuffer* tupleBuffer, const bool isLastChunk)
{
    tupleBuffer->setLastChunk(isLastChunk);
};

NAUT_INLINE inline ChunkNumber NES_Memory_TupleBuffer_getChunkNumber(const TupleBuffer* tupleBuffer)
{
    return tupleBuffer->getChunkNumber();
};

NAUT_INLINE inline bool NES_Memory_TupleBuffer_isLastChunk(const TupleBuffer* tupleBuffer)
{
    return tupleBuffer->isLastChunk();
};

}
