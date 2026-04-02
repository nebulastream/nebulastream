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

#include <Nautilus/Interface/BufferRef/ColumnTupleBufferLayoutRef.hpp>

#include <cstdint>
#include <ranges>
#include <utility>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <Nautilus/Interface/BufferRef/BufferLayoutRef.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <nautilus/static.hpp>
#include <nautilus/val_ptr.hpp>
#include <val.hpp>
#include <val_bool.hpp>

namespace NES
{

ColumnTupleBufferLayoutRef::ColumnTupleBufferLayoutRef(std::vector<Field> fields, const uint64_t tupleSize, const uint64_t bufferSize)
    : fields(std::move(fields))
    , tupleSize(tupleSize)
    , bufferSize(bufferSize)
    , capacity(bufferSize / tupleSize)
{
}

nautilus::val<int8_t*> ColumnTupleBufferLayoutRef::getHeaderStart(const nautilus::val<int8_t*>& bufferBase) const
{
    /// Column layout has no header
    return bufferBase;
}

nautilus::val<int8_t*> ColumnTupleBufferLayoutRef::getDataStart(const nautilus::val<int8_t*>& bufferBase) const
{
    /// No header, data starts at buffer base
    return bufferBase;
}

uint64_t ColumnTupleBufferLayoutRef::getHeaderSize() const
{
    return 0;
}

uint64_t ColumnTupleBufferLayoutRef::getCapacity() const
{
    return capacity;
}

uint64_t ColumnTupleBufferLayoutRef::getBufferSize() const
{
    return bufferSize;
}

uint64_t ColumnTupleBufferLayoutRef::getTupleSize() const
{
    return tupleSize;
}

namespace
{
/// For columnar layout the field address is:
///   dataStart + columnOffset + recordIndex * dataTypeSize
nautilus::val<int8_t*> calculateFieldAddress(
    const nautilus::val<int8_t*>& dataBase,
    nautilus::val<uint64_t>& recordIndex,
    const uint64_t dataTypeSize,
    const uint64_t columnOffset)
{
    const auto fieldOffset = recordIndex * dataTypeSize + columnOffset;
    return dataBase + fieldOffset;
}
} // namespace

Record ColumnTupleBufferLayoutRef::readRecord(
    const std::vector<Record::RecordFieldIdentifier>& projections,
    const RecordBuffer& recordBuffer,
    nautilus::val<uint64_t>& recordIndex) const
{
    Record record;
    const auto base = getDataStart(recordBuffer.getMemArea());
    for (nautilus::static_val<uint64_t> i = 0; i < fields.size(); ++i)
    {
        const auto& [name, type, dataTypeSize, columnOffset] = fields.at(i);
        if (not includesField(projections, name))
        {
            continue;
        }
        auto fieldAddress = calculateFieldAddress(base, recordIndex, dataTypeSize, columnOffset);
        const auto& value = loadValue(type, recordBuffer, fieldAddress);
        record.write(name, value);
    }
    return record;
}

BufferLayoutRef::WriteRecordResult ColumnTupleBufferLayoutRef::writeRecord(
    nautilus::val<uint64_t>& recordIndex,
    const RecordBuffer& recordBuffer,
    const Record& rec,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider) const
{
    nautilus::val<bool> successful{false};
    nautilus::val<uint64_t> writtenRecords{0};
    if (recordIndex < capacity)
    {
        const auto base = getDataStart(recordBuffer.getMemArea());
        for (nautilus::static_val<uint64_t> i = 0; i < fields.size(); ++i)
        {
            const auto& [name, type, dataTypeSize, columnOffset] = fields.at(i);
            if (not rec.hasField(name))
            {
                continue;
            }
            auto fieldAddress = calculateFieldAddress(base, recordIndex, dataTypeSize, columnOffset);
            const auto& value = rec.read(name);
            storeValue(type, recordBuffer, fieldAddress, value, bufferProvider);
        }
        writtenRecords = 1;
        successful = true;
    }
    return {.successful = successful, .writtenRecords = writtenRecords};
}

std::vector<Record::RecordFieldIdentifier> ColumnTupleBufferLayoutRef::getAllFieldNames() const
{
    return fields | std::views::transform([](const Field& f) { return f.name; }) | std::ranges::to<std::vector>();
}

std::vector<DataType> ColumnTupleBufferLayoutRef::getAllDataTypes() const
{
    return fields | std::views::transform([](const Field& f) { return f.type; }) | std::ranges::to<std::vector>();
}

} // namespace NES