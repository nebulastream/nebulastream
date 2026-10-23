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

#include <Interface/HashMap/ChainedHashMap/EntryRecordsRef.hpp>

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <memory>
#include <optional>
#include <utility>
#include <DataTypes/DataType.hpp>
#include <DataTypes/UnboundSchema.hpp>
#include <Interface/NautilusBuffer.hpp>
#include <Interface/PagedVector/PagedVector.hpp>
#include <Interface/PagedVector/PagedVectorRef.hpp>
#include <Interface/Record.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/MemoryUtils.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <ErrorHandling.hpp>
#include <function.hpp>
#include <val.hpp>
#include <val_arith.hpp>
#include <val_bool.hpp>
#include <val_ptr.hpp>

namespace NES
{

namespace
{
/// The start of the value area if there are inline records
struct SmallBufferHeader
{
    uint32_t overflowChildIndex;
    uint32_t numberOfRecords;
};

SmallBufferHeader loadHeader(const int8_t* valueMemArea)
{
    SmallBufferHeader header{};
    std::memcpy(&header, valueMemArea, sizeof(header));
    return header;
}

void storeHeader(int8_t* valueMemArea, const SmallBufferHeader& header)
{
    std::memcpy(valueMemArea, &header, sizeof(header));
}

int8_t* getInlineRecordAddress(int8_t* valueMemArea, const uint64_t position, const uint64_t tupleSize)
{
    return valueMemArea + sizeof(SmallBufferHeader) + (position * tupleSize); /// NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
}

/// Creates a paged vector as a child of the hash map buffer. Its main buffer is a full buffer, which stores the first records.
uint32_t createPagedVector(TupleBuffer* hashMapBuffer, AbstractBufferProvider* bufferProvider, const uint64_t tupleSize)
{
    const auto inlineSizeInBytes = bufferProvider->getBufferSize() - PagedVector::getMainBufferSize();
    auto pagedVectorBuffer = bufferProvider->getBufferBlocking();
    PagedVector::init(pagedVectorBuffer, *bufferProvider, tupleSize, inlineSizeInBytes);
    return hashMapBuffer->storeChildBuffer(pagedVectorBuffer).getRawValue();
}

void initializeProxy(
    TupleBuffer* hashMapBuffer,
    int8_t* valueMemArea,
    AbstractBufferProvider* bufferProvider,
    const uint64_t inlineRecords,
    const uint64_t tupleSize)
{
    if (inlineRecords == 0)
    {
        const auto childIndex = createPagedVector(hashMapBuffer, bufferProvider, tupleSize);
        std::memcpy(valueMemArea, &childIndex, sizeof(childIndex));
        return;
    }
    storeHeader(valueMemArea, {.overflowChildIndex = EntryRecordsRef::NO_OVERFLOW, .numberOfRecords = 0});
}

/// Counts a new record and returns its address, in the small buffer or in the paged vector for the records after the small buffer.
int8_t* reserveRecordSlotProxy(
    TupleBuffer* hashMapBuffer,
    int8_t* valueMemArea,
    AbstractBufferProvider* bufferProvider,
    const uint64_t inlineRecords,
    const uint64_t tupleSize)
{
    auto header = loadHeader(valueMemArea);
    if (header.numberOfRecords < inlineRecords)
    {
        auto* const address = getInlineRecordAddress(valueMemArea, header.numberOfRecords, tupleSize);
        header.numberOfRecords += 1;
        storeHeader(valueMemArea, header);
        return address;
    }
    if (header.overflowChildIndex == EntryRecordsRef::NO_OVERFLOW)
    {
        header.overflowChildIndex = createPagedVector(hashMapBuffer, bufferProvider, tupleSize);
    }
    header.numberOfRecords += 1;
    storeHeader(valueMemArea, header);
    auto pagedVectorBuffer = hashMapBuffer->loadChildBuffer(ChildBufferIndex{header.overflowChildIndex});
    return PagedVector::load(pagedVectorBuffer).reserveRecordSlot(bufferProvider);
}

int8_t* locateRecordProxy(
    const TupleBuffer* hashMapBuffer, int8_t* valueMemArea, const uint64_t position, const uint64_t inlineRecords, const uint64_t tupleSize)
{
    if (position < inlineRecords)
    {
        return getInlineRecordAddress(valueMemArea, position, tupleSize);
    }
    const auto header = loadHeader(valueMemArea);
    const auto pagedVectorBuffer = hashMapBuffer->loadChildBuffer(ChildBufferIndex{header.overflowChildIndex});
    return PagedVector::load(pagedVectorBuffer).locateRecord(position - inlineRecords);
}

uint64_t numberOfRecordsProxy(const int8_t* valueMemArea)
{
    return loadHeader(valueMemArea).numberOfRecords;
}

/// The small buffer only holds records without var sized data, so nothing is loaded or allocated for them
std::pair<nautilus::val<int8_t*>, nautilus::val<uint64_t>> noVarSizedData(const nautilus::val<int8_t*>& /*fieldSlot*/)
{
    nautilus::invoke(+[] { throw UnknownException("Records in the small buffer of a hash map entry cannot have var sized data"); });
    return {nautilus::val<int8_t*>(nullptr), nautilus::val<uint64_t>(0)};
}

nautilus::val<int8_t*> noVarSizedAllocation(const nautilus::val<int8_t*>& /*fieldSlot*/, const nautilus::val<uint64_t>& /*size*/)
{
    nautilus::invoke(+[] { throw UnknownException("Records in the small buffer of a hash map entry cannot have var sized data"); });
    return nautilus::val<int8_t*>(nullptr);
}
}

uint64_t EntryRecordsRef::getEffectiveInlineRecords(
    const Schema<QualifiedUnboundField, Ordered>& schema, const uint64_t requestedInlineRecords, const uint64_t bufferSize)
{
    const bool hasVarSizedData
        = std::ranges::any_of(schema, [](const auto& field) { return field.getDataType().isType(DataType::Type::VARSIZED); });
    const auto tupleSize = getSizeInBytes(schema);
    if (requestedInlineRecords == 0 or hasVarSizedData or tupleSize == 0)
    {
        return 0;
    }
    /// An entry (and thereby the small buffer) should only take a small part of the page of the hash map that stores it.
    const uint64_t maximumValueAreaSize = bufferSize / 8;
    if (maximumValueAreaSize <= sizeof(SmallBufferHeader))
    {
        return 0;
    }
    return std::min<uint64_t>(requestedInlineRecords, (maximumValueAreaSize - sizeof(SmallBufferHeader)) / tupleSize);
}

uint64_t EntryRecordsRef::getValueAreaSize(const uint64_t inlineRecords, const uint64_t tupleSize)
{
    if (inlineRecords == 0)
    {
        return sizeof(uint32_t);
    }
    return sizeof(SmallBufferHeader) + (inlineRecords * tupleSize);
}

void EntryRecordsRef::initialize(
    const BorrowedNautilusBuffer& hashMapBuffer,
    const nautilus::val<int8_t*>& valueMemArea,
    const std::shared_ptr<PagedVectorTupleLayout>& tupleLayout,
    const uint64_t inlineRecords,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider)
{
    auto buffer = hashMapBuffer;
    nautilus::invoke(
        initializeProxy,
        buffer.asArg(),
        valueMemArea,
        bufferProvider,
        nautilus::val<uint64_t>(inlineRecords),
        nautilus::val<uint64_t>(getSizeInBytes(tupleLayout->getSchema())));
}

EntryRecordsRef::EntryRecordsRef(
    const BorrowedNautilusBuffer& hashMapBuffer,
    const nautilus::val<int8_t*>& valueMemArea,
    std::shared_ptr<PagedVectorTupleLayout> tupleLayout,
    const uint64_t inlineRecords)
    : hashMapBuffer(hashMapBuffer)
    , valueMemArea(valueMemArea)
    , tupleLayout(std::move(tupleLayout))
    , inlineRecords(inlineRecords)
    , tupleSize(getSizeInBytes(this->tupleLayout->getSchema()))
{
    if (inlineRecords == 0)
    {
        auto pagedVectorBuffer = this->hashMapBuffer.getChildFromIndexAddress(static_cast<nautilus::val<uint32_t*>>(valueMemArea));
        pagedVector.emplace(std::move(pagedVectorBuffer), this->tupleLayout);
    }
}

void EntryRecordsRef::pushBack(const Record& record, const nautilus::val<AbstractBufferProvider*>& bufferProvider)
{
    if (inlineRecords == 0)
    {
        pagedVector->pushBack(record, bufferProvider);
        return;
    }
    const auto address = nautilus::invoke(
        reserveRecordSlotProxy,
        hashMapBuffer.asArg(),
        valueMemArea,
        bufferProvider,
        nautilus::val<uint64_t>(inlineRecords),
        nautilus::val<uint64_t>(tupleSize));
    tupleLayout->writeRecord(record, address, noVarSizedAllocation);
}

EntryRecordsRef::Iterator EntryRecordsRef::begin() const
{
    if (inlineRecords == 0)
    {
        return Iterator{hashMapBuffer, valueMemArea, tupleLayout, inlineRecords, tupleSize, pagedVector->begin()};
    }
    return Iterator{hashMapBuffer, valueMemArea, tupleLayout, inlineRecords, tupleSize, std::nullopt};
}

EntryRecordsRef::Sentinel EntryRecordsRef::end() const
{
    if (inlineRecords == 0)
    {
        return Sentinel{pagedVector->end()};
    }
    return Sentinel{nautilus::invoke(numberOfRecordsProxy, valueMemArea)};
}

EntryRecordsRef::Iterator::Iterator(
    BorrowedNautilusBuffer hashMapBuffer,
    nautilus::val<int8_t*> valueMemArea,
    std::shared_ptr<PagedVectorTupleLayout> tupleLayout,
    const uint64_t inlineRecords,
    const uint64_t tupleSize,
    std::optional<PagedVectorRefIter> pagedVectorIterator)
    : hashMapBuffer(std::move(hashMapBuffer))
    , valueMemArea(std::move(valueMemArea))
    , tupleLayout(std::move(tupleLayout))
    , inlineRecords(inlineRecords)
    , tupleSize(tupleSize)
    , pagedVectorIterator(std::move(pagedVectorIterator))
{
}

Record EntryRecordsRef::Iterator::operator*() const
{
    if (pagedVectorIterator)
    {
        return **pagedVectorIterator;
    }
    const auto address = nautilus::invoke(
        locateRecordProxy,
        hashMapBuffer.asArg(),
        valueMemArea,
        position,
        nautilus::val<uint64_t>(inlineRecords),
        nautilus::val<uint64_t>(tupleSize));
    return tupleLayout->readRecord(address, noVarSizedData);
}

EntryRecordsRef::Iterator& EntryRecordsRef::Iterator::operator++()
{
    if (pagedVectorIterator)
    {
        ++*pagedVectorIterator;
    }
    else
    {
        position = position + 1;
    }
    return *this;
}

nautilus::val<bool> EntryRecordsRef::Iterator::operator!=(const Sentinel& end) const
{
    if (pagedVectorIterator)
    {
        return *pagedVectorIterator != *end.pagedVectorSentinel;
    }
    return position != end.numberOfRecords;
}

}
