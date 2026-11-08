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

#include <Interface/RowHeap/RowHeap.hpp>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <span>
#include <utility>
#include <vector>
#include <Interface/VariableSizedAccess.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

namespace
{
TupleBuffer allocate(AbstractBufferProvider& bufferProvider, const uint64_t size)
{
    auto buffer = bufferProvider.getUnpooledBuffer(size);
    if (not buffer.has_value())
    {
        throw BufferAllocationFailure("No unpooled TupleBuffer of {}B available for a row heap", size);
    }
    return std::move(buffer).value();
}
}

uint64_t RowHeap::getMainBufferSize(const uint64_t capacity, const uint64_t rowSize)
{
    return sizeof(Header) + ((capacity + 1) * rowSize);
}

void RowHeap::init(
    TupleBuffer& buffer,
    AbstractBufferProvider& bufferProvider,
    const uint64_t capacity,
    const uint64_t rowSize,
    const uint64_t varSizedPageSize,
    const std::span<const uint64_t> varSizedFieldOffsets)
{
    PRECONDITION(capacity > 0, "A row heap needs room for at least one row");
    PRECONDITION(buffer.getBufferSize() >= getMainBufferSize(capacity, rowSize), "The buffer of the row heap is too small");
    PRECONDITION(varSizedFieldOffsets.size() <= MAX_VAR_SIZED_FIELDS, "A row heap supports at most {} variable-sized fields", MAX_VAR_SIZED_FIELDS);
    auto* header = new (buffer.getAvailableMemoryArea<>().data()) Header{
        .magic = VALID_ROW_HEAP,
        .capacity = capacity,
        .rowSize = rowSize,
        .size = 0,
        .varSizedUsed = 0,
        .varSizedCapacity = varSizedPageSize,
        .varSizedPageSize = varSizedPageSize,
        .numberOfVarSizedFields = varSizedFieldOffsets.size(),
        .varSizedFieldOffsets = {}};
    std::ranges::copy(varSizedFieldOffsets, header->varSizedFieldOffsets.begin());
    auto varSized = allocate(bufferProvider, varSizedPageSize);
    const auto index = buffer.storeChildBuffer(varSized);
    INVARIANT(index == ChildBufferIndex{0}, "The variable-sized data of a row heap must be its first child");
}

RowHeap RowHeap::load(const TupleBuffer& buffer)
{
    return RowHeap{buffer};
}

RowHeap::RowHeap(TupleBuffer buffer) : buffer(std::move(buffer))
{
    INVARIANT(header().magic == VALID_ROW_HEAP, "The buffer does not hold a row heap");
}

RowHeap::Header& RowHeap::header() const
{
    return *reinterpret_cast<Header*>(buffer.getAvailableMemoryArea<>().data()); /// NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
}

uint64_t RowHeap::size() const
{
    return header().size;
}

uint64_t RowHeap::capacity() const
{
    return header().capacity;
}

int8_t* RowHeap::row(const uint64_t index) const
{
    PRECONDITION(index <= header().capacity, "Row {} is out of range of a row heap with capacity {}", index, header().capacity);
    auto* rows = buffer.getAvailableMemoryArea<>().data() + sizeof(Header);
    return reinterpret_cast<int8_t*>(rows + (index * header().rowSize)); /// NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
}

int8_t* RowHeap::clearPending()
{
    auto* pending = row(header().capacity);
    std::memset(pending, 0, header().rowSize);
    return pending;
}

VariableSizedAccess RowHeap::storeVarSized(AbstractBufferProvider& bufferProvider, const std::span<const std::byte> value)
{
    auto& heapHeader = header();
    if (heapHeader.varSizedUsed + value.size() > heapHeader.varSizedCapacity)
    {
        compact(bufferProvider, value.size());
    }
    const auto offset = heapHeader.varSizedUsed;
    if (not value.empty())
    {
        auto varSized = buffer.loadChildBuffer(ChildBufferIndex{0});
        std::memcpy(varSized.getAvailableMemoryArea<>().data() + offset, value.data(), value.size());
    }
    heapHeader.varSizedUsed += value.size();
    return VariableSizedAccess{ChildBufferIndex{0}, VariableSizedAccess::Offset{offset}, VariableSizedAccess::Size{value.size()}};
}

void RowHeap::compact(AbstractBufferProvider& bufferProvider, const uint64_t additionalBytes)
{
    auto& heapHeader = header();
    const auto oldVarSized = buffer.loadChildBuffer(ChildBufferIndex{0});
    const auto varSizedFields = std::span{heapHeader.varSizedFieldOffsets}.first(heapHeader.numberOfVarSizedFields);

    /// The rows of the heap and the scratch slot, whose values written so far are live as well.
    std::vector<uint64_t> liveRows;
    for (uint64_t index = 0; index < heapHeader.size; ++index)
    {
        liveRows.push_back(index);
    }
    liveRows.push_back(heapHeader.capacity);

    uint64_t liveBytes = 0;
    for (const auto index : liveRows)
    {
        for (const auto fieldOffset : varSizedFields)
        {
            const auto* access = reinterpret_cast<const VariableSizedAccess*>(row(index) + fieldOffset); /// NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
            liveBytes += access->getSize().getRawSize();
        }
    }

    const auto newCapacity = std::max(2 * (liveBytes + additionalBytes), heapHeader.varSizedPageSize);
    auto newVarSized = allocate(bufferProvider, newCapacity);
    uint64_t used = 0;
    for (const auto index : liveRows)
    {
        for (const auto fieldOffset : varSizedFields)
        {
            auto* access = reinterpret_cast<VariableSizedAccess*>(row(index) + fieldOffset); /// NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
            const auto valueSize = access->getSize().getRawSize();
            if (valueSize == 0)
            {
                continue;
            }
            std::memcpy(
                newVarSized.getAvailableMemoryArea<>().data() + used,
                oldVarSized.getAvailableMemoryArea<>().data() + access->getOffset().getRawOffset(),
                valueSize);
            *access = VariableSizedAccess{ChildBufferIndex{0}, VariableSizedAccess::Offset{used}, VariableSizedAccess::Size{valueSize}};
            used += valueSize;
        }
    }
    heapHeader.varSizedUsed = used;
    heapHeader.varSizedCapacity = newCapacity;
    buffer.replaceChildBuffer(ChildBufferIndex{0}, newVarSized);
}

void RowHeap::swapRows(const uint64_t first, const uint64_t second)
{
    const auto rowSize = header().rowSize;
    std::swap_ranges(row(first), row(first) + rowSize, row(second));
}

void RowHeap::siftUp(uint64_t index, const Comparator& comesBefore)
{
    /// The root is the row that comes last: a parent must not come before its children.
    while (index > 0)
    {
        const auto parent = (index - 1) / 2;
        if (not comesBefore(row(parent), row(index)))
        {
            return;
        }
        swapRows(parent, index);
        index = parent;
    }
}

void RowHeap::siftDown(uint64_t index, const Comparator& comesBefore)
{
    const auto size = header().size;
    while (true)
    {
        auto last = index;
        for (const auto child : {(2 * index) + 1, (2 * index) + 2})
        {
            if (child < size and comesBefore(row(last), row(child)))
            {
                last = child;
            }
        }
        if (last == index)
        {
            return;
        }
        swapRows(index, last);
        index = last;
    }
}

void RowHeap::pushPending(const Comparator& comesBefore)
{
    auto& heapHeader = header();
    PRECONDITION(heapHeader.size < heapHeader.capacity, "Cannot push onto a full row heap");
    std::memcpy(row(heapHeader.size), row(heapHeader.capacity), heapHeader.rowSize);
    ++heapHeader.size;
    siftUp(heapHeader.size - 1, comesBefore);
}

void RowHeap::replacePendingTop(const Comparator& comesBefore)
{
    auto& heapHeader = header();
    PRECONDITION(heapHeader.size == heapHeader.capacity, "Only a full row heap replaces its top row");
    /// The values of the replaced row become garbage; the next compaction drops them.
    std::memcpy(row(0), row(heapHeader.capacity), heapHeader.rowSize);
    siftDown(0, comesBefore);
}

void RowHeap::sortRows(const Comparator& comesBefore)
{
    const auto& heapHeader = header();
    std::vector<uint64_t> order(heapHeader.size);
    for (uint64_t index = 0; index < order.size(); ++index)
    {
        order[index] = index;
    }
    std::ranges::sort(order, [&](const uint64_t first, const uint64_t second) { return comesBefore(row(first), row(second)); });
    std::vector<int8_t> sorted(heapHeader.size * heapHeader.rowSize);
    for (uint64_t position = 0; position < order.size(); ++position)
    {
        std::memcpy(sorted.data() + (position * heapHeader.rowSize), row(order[position]), heapHeader.rowSize);
    }
    if (not sorted.empty())
    {
        std::memcpy(row(0), sorted.data(), sorted.size());
    }
}

uint64_t RowHeap::getVarSizedBytesUsed() const
{
    return header().varSizedUsed;
}

uint64_t RowHeap::getVarSizedCapacity() const
{
    return header().varSizedCapacity;
}

}
