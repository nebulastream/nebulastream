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

#include <cstdint>
#include <utility>
#include <MemoryLayout/MemoryLayoutTupleBuffer.hpp>
#include <Runtime/TupleBuffer.hpp>

namespace NES::Memory::MemoryLayouts
{
MemoryLayoutTupleBuffer::MemoryLayoutTupleBuffer(Memory::TupleBuffer tupleBuffer, const uint64_t capacity)
    : tupleBuffer(std::move(tupleBuffer)), capacity(capacity)
{
}

uint64_t MemoryLayoutTupleBuffer::getCapacity() const
{
    return capacity;
}

uint64_t MemoryLayoutTupleBuffer::getNumberOfRecords() const
{
    return numberOfRecords;
}

Memory::TupleBuffer MemoryLayoutTupleBuffer::getTupleBuffer()
{
    return tupleBuffer;
}
}
