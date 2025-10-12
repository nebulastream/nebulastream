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

namespace NES::ProxyFunctions
{
inline int8_t* NES_Memory_TupleBuffer_getMemArea(TupleBuffer* tupleBuffer)
{
    return reinterpret_cast<int8_t*>(tupleBuffer->getAvailableMemoryArea().data());
};

inline uint64_t NES_Memory_TupleBuffer_getBufferSize(const TupleBuffer* tupleBuffer)
{
    return tupleBuffer->getBufferSize();
};

inline uint64_t NES_Memory_TupleBuffer_getNumberOfTuples(const TupleBuffer* tupleBuffer)
{
    return tupleBuffer->getNumberOfTuples();
};

void inline NES_Memory_TupleBuffer_setNumberOfTuples(TupleBuffer* tupleBuffer, const uint64_t numberOfTuples)
{
    tupleBuffer->setNumberOfTuples(numberOfTuples);
}

inline OriginId NES_Memory_TupleBuffer_getOriginId(const TupleBuffer* tupleBuffer)
{
    return tupleBuffer->getOriginId();
};

inline void NES_Memory_TupleBuffer_setOriginId(TupleBuffer* tupleBuffer, const OriginId value)
{
    tupleBuffer->setOriginId(OriginId(value));
};

inline Timestamp NES_Memory_TupleBuffer_getWatermark(const TupleBuffer* tupleBuffer)
{
    return tupleBuffer->getWatermark();
};

inline void NES_Memory_TupleBuffer_setWatermark(TupleBuffer* tupleBuffer, const Timestamp value)
{
    tupleBuffer->setWatermark(Timestamp(value));
};

inline Timestamp NES_Memory_TupleBuffer_getCreationTimestampInMS(const TupleBuffer* tupleBuffer)
{
    return tupleBuffer->getCreationTimestampInMS();
};

inline void NES_Memory_TupleBuffer_setSequenceNumber(TupleBuffer* tupleBuffer, const SequenceNumber sequenceNumber)
{
    tupleBuffer->setSequenceNumber(sequenceNumber);
};

inline SequenceNumber NES_Memory_TupleBuffer_getSequenceNumber(const TupleBuffer* tupleBuffer)
{
    return tupleBuffer->getSequenceNumber();
}

inline void NES_Memory_TupleBuffer_setCreationTimestampInMS(TupleBuffer* tupleBuffer, const Timestamp value)
{
    tupleBuffer->setCreationTimestampInMS(Timestamp(value));
}

inline void NES_Memory_TupleBuffer_setChunkNumber(TupleBuffer* tupleBuffer, const ChunkNumber chunkNumber)
{
    tupleBuffer->setChunkNumber(ChunkNumber(chunkNumber));
};

inline void NES_Memory_TupleBuffer_setLastChunk(TupleBuffer* tupleBuffer, const bool isLastChunk)
{
    tupleBuffer->setLastChunk(isLastChunk);
};

inline ChunkNumber NES_Memory_TupleBuffer_getChunkNumber(const TupleBuffer* tupleBuffer)
{
    return tupleBuffer->getChunkNumber();
};

inline bool NES_Memory_TupleBuffer_isLastChunk(const TupleBuffer* tupleBuffer)
{
    return tupleBuffer->isLastChunk();
};

}
