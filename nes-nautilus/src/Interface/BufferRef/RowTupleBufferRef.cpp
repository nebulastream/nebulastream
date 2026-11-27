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
#include <Interface/BufferRef/RowTupleBufferRef.hpp>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <ranges>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <Interface/BufferRef/TupleBufferRef.hpp>
#include <Interface/Record.hpp>
#include <Interface/RecordBuffer.hpp>
#include <Interface/RecordLayoutUtil.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <nautilus/val_ptr.hpp>
#include <InternalCodec.hpp>
#include <InternalCodecProvider.hpp>
#include <val.hpp>
#include <val_bool.hpp>

namespace NES
{

RowTupleBufferRef::RowTupleBufferRef(std::vector<Field> fields, const uint64_t tupleSize, const uint64_t bufferSize)
    : TupleBufferRef(bufferSize / tupleSize, bufferSize, tupleSize), fields(std::move(fields))
{
}

Record RowTupleBufferRef::readRecord(
    const std::vector<Record::RecordFieldIdentifier>& projections,
    const RecordBuffer& recordBuffer,
    nautilus::val<uint64_t>& recordIndex) const
{
    /// Row layout: the record occupies a contiguous slice; each field sits at its precomputed offset.
    const auto recordAddress = recordBuffer.getMemArea() + (tupleSize * recordIndex);
    const auto access = FieldAccess::createFieldAccesses(
        fields,
        [recordAddress](const auto& field)
        {
            const std::shared_ptr<InternalCodec> noCodec = createInternalCodec(field.type, "None");
            return FieldAccess{field.name, field.type, recordAddress + nautilus::val<uint64_t>(field.fieldOffset), noCodec};
        },
        [&projections](const auto& field) { return includesField(projections, field.name); });
    return readRecordFields(access, getRecordBufferLoad(recordBuffer));
}

TupleBufferRef::WriteRecordResult RowTupleBufferRef::writeRecord(
    nautilus::val<uint64_t>& recordIndex,
    const RecordBuffer& recordBuffer,
    const Record& rec,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    std::unordered_map<Record::RecordFieldIdentifier, std::shared_ptr<InternalCodec>> internalCodecMap) const
{
    nautilus::val<bool> successful{false};
    nautilus::val<uint64_t> writtenRecords{0};
    /// Check if index is in-bounds
    if (recordIndex < capacity)
    {
        /// We do not encode in row layout, so the record address can be calculated like this despite the new internal codec options
        const auto recordAddress = recordBuffer.getMemArea() + (tupleSize * recordIndex);
        const auto access = FieldAccess::createFieldAccesses(
            fields,
            [recordAddress, internalCodecMap](const auto& field)
            {
                return FieldAccess{
                    field.name, field.type, recordAddress + nautilus::val<uint64_t>(field.fieldOffset), internalCodecMap.at(field.name)};
            });
        writeRecordFields(access, rec, getRecordBufferStore(recordBuffer, bufferProvider));
        writtenRecords = 1;
        successful = true;
    }
    return {.successful = successful, .writtenRecords = writtenRecords};
}

std::vector<Record::RecordFieldIdentifier> RowTupleBufferRef::getAllFieldNames() const
{
    return fields | std::views::transform([](const Field& field) { return field.name; }) | std::ranges::to<std::vector>();
}

std::vector<DataType> RowTupleBufferRef::getAllDataTypes() const
{
    return fields | std::views::transform([](const Field& field) { return field.type; }) | std::ranges::to<std::vector>();
}

std::vector<std::string> RowTupleBufferRef::getAllInternalCodecs() const
{
    /// ROW_LAYOUT is currently not elligable for encodings.
    return fields | std::views::transform([](const Field&) { return std::string("None"); }) | std::ranges::to<std::vector>();
}

}
