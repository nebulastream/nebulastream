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
#include <Nautilus/Interface/BufferRef/RowTupleBufferRef.hpp>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>
#include <vector>
#include <DataTypes/Schema.hpp>
#include <MemoryLayout/MemoryLayout.hpp>
#include <MemoryLayout/RowLayout.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <nautilus/val_ptr.hpp>
#include <static.hpp>
#include <val.hpp>

namespace NES
{

RowTupleBufferRef::RowTupleBufferRef(std::shared_ptr<RowLayout> rowMemoryLayout) : rowMemoryLayout(std::move(rowMemoryLayout)) { };

std::shared_ptr<MemoryLayout> RowTupleBufferRef::getMemoryLayout() const
{
    return rowMemoryLayout;
}

nautilus::val<int8_t*> RowTupleBufferRef::calculateFieldAddress(const nautilus::val<int8_t*>& recordOffset, const uint64_t fieldIndex) const
{
    auto fieldOffset = rowMemoryLayout->getFieldOffset(fieldIndex);
    auto fieldAddress = recordOffset + nautilus::val<uint64_t>(fieldOffset);
    return fieldAddress;
}

Record RowTupleBufferRef::readRecord(
    const std::vector<Record::RecordFieldIdentifier>& projections,
    const RecordBuffer& recordBuffer,
    nautilus::val<uint64_t>& recordIndex) const
{
    /// read all fields
    const auto& schema = rowMemoryLayout->getSchema();
    Record record;
    const auto tupleSize = rowMemoryLayout->getTupleSize();
    const auto bufferAddress = recordBuffer.getMemArea();
    const auto recordOffset = bufferAddress + (tupleSize * recordIndex);
    for (nautilus::static_val<uint64_t> i = 0; i < schema.getNumberOfFields(); ++i)
    {
        const auto& fieldName = schema.getFieldAt(i).name;
        if (!includesField(projections, fieldName))
        {
            continue;
        }
        auto fieldAddress = calculateFieldAddress(recordOffset, i);
        auto value = loadValue(rowMemoryLayout->getPhysicalType(i), recordBuffer, fieldAddress);
        record.write(rowMemoryLayout->getSchema().getFieldAt(i).name, value);
    }
    return record;
}

void RowTupleBufferRef::writeRecord(
    nautilus::val<uint64_t>& recordIndex,
    const RecordBuffer& recordBuffer,
    const Record& rec,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider) const
{
    auto tupleSize = rowMemoryLayout->getTupleSize();
    const auto bufferAddress = recordBuffer.getMemArea();
    const auto recordOffset = bufferAddress + (tupleSize * recordIndex);
    const auto schema = rowMemoryLayout->getSchema();

    const nautilus::val<uint64_t> varSizedOffset = 0;
    for (nautilus::static_val<size_t> i = 0; i < schema.getNumberOfFields(); ++i)
    {
        auto fieldAddress = calculateFieldAddress(recordOffset, i);
        const auto& value = rec.read(schema.getFieldAt(i).name);
        storeValue(rowMemoryLayout->getPhysicalType(i), recordBuffer, fieldAddress, value, bufferProvider);
    }
}

}
