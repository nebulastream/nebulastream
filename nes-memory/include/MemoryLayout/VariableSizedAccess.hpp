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

#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

/// @brief This class is a helper class for accessing variable sized data in a tuple buffer.
/// We store variable sized data as child buffer. To reference one variable sized data object, we require an index to the child buffer and
/// an offset in the child buffer locating the start of the variable sized data object.
///
/// We use 64 bits to store the child index and offset for accessing the correct variable sized data.
/// We use the upper 32 bits for the childIndex and the lower 32 bits for the childBufferOffset
/// This allows us to have 4 million child buffer and having a maximum child buffer size of 4 GB
/// (unless we have only one var sized object per child)
class VariableSizedAccess
{
public:
    using Index = NESStrongType<uint32_t, struct VariableSizedAccessIndex_, std::numeric_limits<uint32_t>::max(), 0>;
    using Offset = NESStrongType<uint32_t, struct VariableSizedAccessOffset_, std::numeric_limits<uint32_t>::max(), 0>;
    using Size = NESStrongType<uint64_t, struct VariableSizedAccessSize_, 0, 1>;

    [[nodiscard]] static VariableSizedAccess readFrom(const TupleBuffer& buffer, size_t offset);
    [[nodiscard]] static VariableSizedAccess readFrom(const TupleBuffer& buffer, const int8_t* fieldReference);
    [[nodiscard]]
    static VariableSizedAccess
    attachTo(TupleBuffer& buffer, AbstractBufferProvider& bufferProvider, std::span<const std::byte> variableSizeData);
    static VariableSizedAccess
    writeTo(TupleBuffer& buffer, size_t offset, AbstractBufferProvider& bufferProvider, std::span<const std::byte> variableSizeData);
    static VariableSizedAccess writeTo(
        TupleBuffer& buffer,
        const int8_t* fieldReference,
        AbstractBufferProvider& bufferProvider,
        std::span<const std::byte> variableSizeData);

    [[nodiscard]] std::span<const std::byte> access(const TupleBuffer& buffer) const;
    [[nodiscard]] std::span<std::byte> access(TupleBuffer& buffer) const;
    [[nodiscard]] Index getIndex() const;
    [[nodiscard]] Offset getOffset() const;
    [[nodiscard]] Size getSize() const;

private:
    static std::optional<VariableSizedAccess> tryAllocation(const TupleBuffer& tupleBuffer, size_t allocationSize);
    static VariableSizedAccess allocateAtLeast(TupleBuffer& tupleBuffer, AbstractBufferProvider& bufferProvider, size_t allocationSize);

    VariableSizedAccess(Index index, Offset offset, Size size) : index(std::move(index)), offset(std::move(offset)), size(std::move(size))
    {
    }

    Index index = INVALID<Index>;
    Offset offset = INVALID<Offset>;
    Size size = INVALID<Size>;
};

}
