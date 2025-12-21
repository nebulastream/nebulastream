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
#include <MemoryLayout/VariableSizedAccess.hpp>

#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <ranges>
#include <span>
#include <utility>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

std::optional<VariableSizedAccess> VariableSizedAccess::tryAllocation(const TupleBuffer& tupleBuffer, size_t allocationSize)
{
    const auto numberOfChildBuffers = tupleBuffer.getNumberOfChildBuffers();
    if (numberOfChildBuffers == 0)
    {
        /// No child buffer yet
        return std::nullopt;
    }

    /// We only check the last child buffer for left over capacities
    auto lastChildBuffer = tupleBuffer.loadChildBuffer(numberOfChildBuffers - 1);
    if (lastChildBuffer.getBufferSize() - lastChildBuffer.getNumberOfTuples() < allocationSize)
    {
        return std::nullopt;
    }

    const auto offset = lastChildBuffer.getNumberOfTuples();
    lastChildBuffer.setNumberOfTuples(offset + allocationSize);
    return VariableSizedAccess{Index(numberOfChildBuffers - 1), Offset(offset), Size(allocationSize)};
}

VariableSizedAccess
VariableSizedAccess::allocateAtLeast(TupleBuffer& tupleBuffer, AbstractBufferProvider& bufferProvider, size_t allocationSize)
{
    if (allocationSize > bufferProvider.getBufferSize())
    {
        auto unpooledBuffer = bufferProvider.getUnpooledBuffer(allocationSize);
        if (!unpooledBuffer)
        {
            throw CannotAllocateBuffer("Cannot allocate memory for variable sized data of size: {}", allocationSize);
        }
        unpooledBuffer->setNumberOfTuples(allocationSize);
        auto index = tupleBuffer.storeChildBuffer(*unpooledBuffer);

        return VariableSizedAccess{Index(index), Offset(0), Size(allocationSize)};
    }

    auto buffer = bufferProvider.getBufferBlocking();
    buffer.setNumberOfTuples(allocationSize);
    auto index = tupleBuffer.storeChildBuffer(buffer);
    return VariableSizedAccess{Index(index), Offset(0), Size(allocationSize)};
}

VariableSizedAccess VariableSizedAccess::readFrom(const TupleBuffer& buffer, size_t offset)
{
    INVARIANT(
        buffer.getBufferSize() > offset,
        "Out of bound access into tuple buffer. BufferSize: {} access at {}",
        buffer.getBufferSize(),
        offset);
    std::array<std::byte, sizeof(VariableSizedAccess)> arr;
    std::ranges::copy(buffer.getAvailableMemoryArea<std::byte>().subspan(offset, sizeof(VariableSizedAccess)), arr.begin());
    return std::bit_cast<VariableSizedAccess>(arr);
}

VariableSizedAccess VariableSizedAccess::readFrom(const TupleBuffer& buffer, const int8_t* fieldReference)
{
    const auto offset = fieldReference - buffer.getAvailableMemoryArea<int8_t>().data();
    return readFrom(buffer, offset);
}

VariableSizedAccess
VariableSizedAccess::attachTo(TupleBuffer& buffer, AbstractBufferProvider& bufferProvider, std::span<const std::byte> variableSizeData)
{
    const auto allocation
        = tryAllocation(buffer, variableSizeData.size())
              .or_else([&] { return std::make_optional(allocateAtLeast(buffer, bufferProvider, variableSizeData.size())); })
              .value();
    std::ranges::copy(variableSizeData, allocation.access(buffer).begin());
    return allocation;
}

VariableSizedAccess VariableSizedAccess::writeTo(
    TupleBuffer& buffer, const int8_t* fieldReference, AbstractBufferProvider& bufferProvider, std::span<const std::byte> variableSizeData)
{
    const auto offset = fieldReference - buffer.getAvailableMemoryArea<int8_t>().data();
    return writeTo(buffer, offset, bufferProvider, variableSizeData);
}

VariableSizedAccess VariableSizedAccess::writeTo(
    TupleBuffer& buffer, size_t offset, AbstractBufferProvider& bufferProvider, std::span<const std::byte> variableSizeData)
{
    auto access = attachTo(buffer, bufferProvider, variableSizeData);
    auto arr = std::bit_cast<std::array<std::byte, sizeof(VariableSizedAccess)>>(access);
    std::ranges::copy(arr, buffer.getAvailableMemoryArea<std::byte>().subspan(offset, sizeof(VariableSizedAccess)).begin());
    return access;
}

std::span<const std::byte> VariableSizedAccess::access(const TupleBuffer& buffer) const
{
    const auto childBuffer = buffer.loadChildBuffer(index.getRawValue());
    return childBuffer.getAvailableMemoryArea().subspan(offset.getRawValue(), size.getRawValue());
}

std::span<std::byte> VariableSizedAccess::access(TupleBuffer& buffer) const
{
    auto childBuffer = buffer.loadChildBuffer(index.getRawValue());
    return childBuffer.getAvailableMemoryArea().subspan(offset.getRawValue(), size.getRawValue());
}

VariableSizedAccess::Index VariableSizedAccess::getIndex() const
{
    return index;
}

VariableSizedAccess::Offset VariableSizedAccess::getOffset() const
{
    return offset;
}

VariableSizedAccess::Size VariableSizedAccess::getSize() const
{
    return size;
}
}
