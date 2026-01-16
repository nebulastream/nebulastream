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
#include <ColumnTupleBufferRef.hpp>

#include <cstdint>
#include <memory>
#include <ranges>
#include <utility>
#include <vector>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <TupleBufferRef.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <nautilus/static.hpp>
#include <nautilus/val_ptr.hpp>
#include <val.hpp>

namespace NES
{

ColumnTupleBufferRef::ColumnTupleBufferRef(std::vector<Field> fields, const uint64_t tupleSize, const uint64_t bufferSize)
    : TupleBufferRef(bufferSize / tupleSize, bufferSize, tupleSize), fields(std::move(fields))
{
}

namespace
{
nautilus::val<int8_t*> calculateFieldAddress(
    const nautilus::val<int8_t*>& bufferAddress,
    nautilus::val<uint64_t>& recordIndex,
    const uint64_t fieldSize,
    const uint64_t columnOffset)
{
    const auto fieldOffset = recordIndex * fieldSize + columnOffset;
    auto fieldAddress = bufferAddress + fieldOffset;
    return fieldAddress;
}
}

Record ColumnTupleBufferRef::readRecord(
    const std::vector<Record::RecordFieldIdentifier>& projections,
    const RecordBuffer& recordBuffer,
    nautilus::val<uint64_t>& recordIndex) const
{
    Record record;
    const auto bufferAddress = recordBuffer.getMemArea();
    for (nautilus::static_val<uint64_t> i = 0; i < fields.size(); ++i)
    {
        const auto& [name, type, columnOffset] = fields.at(i);
        if (not includesField(projections, name))
        {
            continue;
        }
        auto fieldAddress = calculateFieldAddress(bufferAddress, recordIndex, type.getSizeInBytes(), columnOffset);
        const auto& value = loadValue(type, recordBuffer, fieldAddress);
        record.write(name, value);
    }
    return record;
}

void ColumnTupleBufferRef::writeRecord(
    nautilus::val<uint64_t>& recordIndex,
    const RecordBuffer& recordBuffer,
    const Record& rec,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider) const
{
    const auto bufferAddress = recordBuffer.getMemArea();
    for (nautilus::static_val<uint64_t> i = 0; i < fields.size(); ++i)
    {
        const auto& [name, type, columnOffset] = fields.at(i);
        if (not rec.hasField(name))
        {
            /// Skipping any fields that are not part of the record
            continue;
        }
        auto fieldAddress = calculateFieldAddress(bufferAddress, recordIndex, type.getSizeInBytes(), columnOffset);
        const auto& value = rec.read(name);
        storeValue(type, recordBuffer, fieldAddress, value, bufferProvider);
    }
}

std::vector<Record::RecordFieldIdentifier> ColumnTupleBufferRef::getAllFieldNames() const
{
    return fields | std::views::transform([](const Field& field) { return field.name; }) | std::ranges::to<std::vector>();
}

std::vector<DataType> ColumnTupleBufferRef::getAllDataTypes() const
{
    return fields | std::views::transform([](const Field& field) { return field.type; }) | std::ranges::to<std::vector>();
}

}
