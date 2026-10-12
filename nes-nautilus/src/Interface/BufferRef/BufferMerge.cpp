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

#include <Interface/BufferRef/BufferMerge.hpp>

#include <cstdint>
#include <cstring>
#include <span>
#include <type_traits>
#include <Interface/VariableSizedAccess.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

namespace
{
static_assert(std::is_trivially_copyable_v<VariableSizedAccess>, "References are copied bytewise");

/// Adds `childShift` to the child index and `offsetShift` to the offset of every variable sized reference of the tuples
/// [targetTuples, targetTuples + sourceTuples).
void relocateReferences(
    const std::span<uint8_t> destination,
    const uint64_t targetTuples,
    const uint64_t sourceTuples,
    const BufferLayout& layout,
    const uint32_t childShift,
    const uint32_t offsetShift)
{
    for (const auto& reference : layout.references)
    {
        for (uint64_t i = targetTuples; i < targetTuples + sourceTuples; ++i)
        {
            /// The reference may be unaligned.
            auto* const slot = destination.subspan(reference.offset + (i * reference.stride), sizeof(VariableSizedAccess)).data();
            VariableSizedAccess access;
            std::memcpy(&access, slot, sizeof(access));
            access = VariableSizedAccess(
                ChildBufferIndex(access.getIndex().getRawValue() + childShift),
                VariableSizedAccess::Offset(access.getOffset().getRawOffset() + offsetShift),
                access.getSize());
            std::memcpy(slot, &access, sizeof(access));
        }
    }
}
}

void appendTuples(TupleBuffer& target, const TupleBuffer& source, const BufferLayout& layout)
{
    const auto targetTuples = target.getNumberOfTuples();
    const auto sourceTuples = source.getNumberOfTuples();
    PRECONDITION(
        targetTuples + sourceTuples <= layout.capacity,
        "{} and {} tuples exceed the capacity of {}",
        targetTuples,
        sourceTuples,
        layout.capacity);
    PRECONDITION(
        source.getBufferSize() >= layout.bufferSize && target.getBufferSize() >= layout.bufferSize,
        "A buffer is smaller than the {} bytes of its layout",
        layout.bufferSize);
    if (sourceTuples == 0)
    {
        return;
    }
    target.setNumberOfTuples(targetTuples + sourceTuples);

    const auto destination = target.getAvailableMemoryArea<uint8_t>();
    const auto origin = source.getAvailableMemoryArea<uint8_t>();

    for (const auto& segment : layout.segments)
    {
        const auto bytes = sourceTuples * segment.stride;
        std::memcpy(
            destination.subspan(segment.offset + (targetTuples * segment.stride), bytes).data(),
            origin.subspan(segment.offset, bytes).data(),
            bytes);
    }

    if (layout.references.empty())
    {
        return;
    }

    const auto targetChildren = target.getNumberOfChildBuffers();
    const auto sourceChildren = source.getNumberOfChildBuffers();
    if (sourceChildren == 1 && targetChildren > 0)
    {
        auto last = target.loadChildBuffer(ChildBufferIndex(targetChildren - 1));
        auto payload = source.loadChildBuffer(ChildBufferIndex(0));
        const auto used = last.getNumberOfTuples();
        const auto bytes = payload.getNumberOfTuples();
        /// Buffer sizes are 32 bits wide, so the shifted offsets cannot overflow.
        if (used + bytes < last.getBufferSize())
        {
            std::memcpy(
                last.getAvailableMemoryArea<uint8_t>().subspan(used, bytes).data(),
                payload.getAvailableMemoryArea<uint8_t>().first(bytes).data(),
                bytes);
            last.setNumberOfTuples(used + bytes);
            relocateReferences(destination, targetTuples, sourceTuples, layout, targetChildren - 1, static_cast<uint32_t>(used));
            return;
        }
    }

    for (uint32_t child = 0; child < sourceChildren; ++child)
    {
        auto adopted = source.loadChildBuffer(ChildBufferIndex(child));
        USED_IN_DEBUG const auto index = target.storeChildBuffer(adopted);
        INVARIANT(index == ChildBufferIndex(targetChildren + child), "Adopted child {} is stored at {}", child, index);
    }

    if (targetChildren > 0)
    {
        relocateReferences(destination, targetTuples, sourceTuples, layout, targetChildren, 0);
    }
}

}
