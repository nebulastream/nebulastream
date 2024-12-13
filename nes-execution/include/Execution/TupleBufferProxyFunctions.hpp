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

#include <Runtime/TupleBuffer.hpp>
namespace NES::Runtime::ProxyFunctions
{
int8_t* NES__Memory__TupleBuffer__getBuffer(Memory::TupleBuffer* tupleBuffer)
{
    return tupleBuffer->getBuffer();
};

uint64_t NES__Memory__TupleBuffer__getBufferSize(Memory::TupleBuffer* tupleBuffer)
{
    return tupleBuffer->getBufferSize();
};

uint64_t NES__Memory__TupleBuffer__getNumberOfTuples(Memory::TupleBuffer* tupleBuffer)
{
    return tupleBuffer->getNumberOfTuples();
};

extern "C" __attribute__((always_inline)) void
NES__Memory__TupleBuffer__setNumberOfTuples(Memory::TupleBuffer* tupleBuffer, uint64_t numberOfTuples)
{
    tupleBuffer->setNumberOfTuples(numberOfTuples);
}

uint64_t NES__Memory__TupleBuffer__getOriginId(Memory::TupleBuffer* tupleBuffer)
{
    return tupleBuffer->getOriginId().getRawValue();
};

void NES__Memory__TupleBuffer__setOriginId(Memory::TupleBuffer* tupleBuffer, uint64_t value)
{
    tupleBuffer->setOriginId(OriginId(value));
};

uint64_t NES__Memory__TupleBuffer__getWatermark(Memory::TupleBuffer* tupleBuffer)
{
    return tupleBuffer->getWatermark();
};

void NES__Memory__TupleBuffer__setWatermark(Memory::TupleBuffer* tupleBuffer, uint64_t value)
{
    tupleBuffer->setWatermark(value);
};

uint64_t NES__Memory__TupleBuffer__getCreationTimestampInMS(Memory::TupleBuffer* tupleBuffer)
{
    return tupleBuffer->getCreationTimestampInMS();
};

void NES__Memory__TupleBuffer__setSequenceNumber(Memory::TupleBuffer* tupleBuffer, uint64_t sequenceNumber)
{
    return tupleBuffer->setSequenceNumber(sequenceNumber);
};

uint64_t NES__Memory__TupleBuffer__getSequenceNumber(Memory::TupleBuffer* tupleBuffer)
{
    return tupleBuffer->getSequenceNumber();
}

void NES__Memory__TupleBuffer__setCreationTimestampInMS(Memory::TupleBuffer* tupleBuffer, uint64_t value)
{
    return tupleBuffer->setCreationTimestampInMS(value);
}

void NES__Memory__TupleBuffer__setChunkNumber(Memory::TupleBuffer* tupleBuffer, uint64_t chunkNumber)
{
    return tupleBuffer->setChunkNumber(chunkNumber);
};

void NES__Memory__TupleBuffer__setLastChunk(Memory::TupleBuffer* tupleBuffer, bool isLastChunk)
{
    return tupleBuffer->setLastChunk(isLastChunk);
};

uint64_t NES__Memory__TupleBuffer__getChunkNumber(Memory::TupleBuffer* tupleBuffer)
{
    return tupleBuffer->getChunkNumber();
};

bool NES__Memory__TupleBuffer__isLastChunk(Memory::TupleBuffer* tupleBuffer)
{
    return tupleBuffer->isLastChunk();
};

}
