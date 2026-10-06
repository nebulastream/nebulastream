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
#include <Interface/NautilusBuffer.hpp>
#include <nautilus/std/cstring.h>
#include <val.hpp>
#include <val_ptr.hpp>

namespace NES
{
/// A sequential binary layout over a TupleBuffer. Its cursor and memory accesses
/// are Nautilus values, so the caller's loops and field copies are compiled.
/// The caller provides a buffer large enough for the encoded state.
class PipelineStateBufferRef
{
public:
    explicit PipelineStateBufferRef(BorrowedNautilusBuffer buffer) : memory(buffer.data()), offset(uint64_t{0}) { }

    void writeU64(const nautilus::val<uint64_t>& value)
    {
        *static_cast<nautilus::val<uint64_t*>>(memory + offset) = value;
        offset = offset + sizeof(uint64_t);
    }

    [[nodiscard]] nautilus::val<uint64_t> readU64()
    {
        auto value = *static_cast<nautilus::val<uint64_t*>>(memory + offset);
        offset = offset + sizeof(uint64_t);
        return value;
    }

    void writeBytes(const nautilus::val<const int8_t*>& source, const uint64_t size)
    {
        nautilus::memcpy(memory + offset, source, size);
        offset = offset + size;
    }

    void readBytes(const nautilus::val<int8_t*>& destination, const uint64_t size)
    {
        nautilus::memcpy(destination, memory + offset, size);
        offset = offset + size;
    }

    [[nodiscard]] nautilus::val<uint64_t> position() const { return offset; }

private:
    nautilus::val<int8_t*> memory;
    nautilus::val<uint64_t> offset;
};
}
