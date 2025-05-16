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

#include <cstddef>
#include <cstdint>
#include <utility>
#include <vector>
#include <API/AttributeField.hpp>
#include <MemoryLayout/ColumnLayout.hpp>
#include <MemoryLayout/MemoryLayout.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/MemoryProvider/ColumnTupleBufferMemoryProvider.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <nautilus/static.hpp>
#include <nautilus/val_ptr.hpp>

namespace NES::Nautilus::Interface::MemoryProvider
{

ColumnTupleBufferMemoryProvider::ColumnTupleBufferMemoryProvider(std::shared_ptr<Memory::MemoryLayouts::ColumnLayout> columnMemoryLayoutPtr)
    : columnMemoryLayout(std::move(std::move(columnMemoryLayoutPtr))) { };

std::shared_ptr<Memory::MemoryLayouts::MemoryLayout> ColumnTupleBufferMemoryProvider::getMemoryLayout() const
{
    return columnMemoryLayout;
}

nautilus::val<int8_t*> ColumnTupleBufferMemoryProvider::calculateFieldAddress(
    const nautilus::val<int8_t*>& bufferAddress, nautilus::val<uint64_t>& recordIndex, const uint64_t fieldIndex) const
{
    auto fieldSize = columnMemoryLayout->getFieldSize(fieldIndex);
    auto columnOffset = columnMemoryLayout->getColumnOffset(fieldIndex);
    const auto fieldOffset = recordIndex * fieldSize + columnOffset;
    auto fieldAddress = bufferAddress + fieldOffset;
    return fieldAddress;
}

Record ColumnTupleBufferMemoryProvider::readRecord(
    const std::vector<Record::RecordFieldIdentifier>& projections,
    const RecordBuffer& recordBuffer,
    nautilus::val<uint64_t>& recordIndex) const
{
    const auto& schema = columnMemoryLayout->getSchema();
    /// read all fields
    const auto bufferAddress = recordBuffer.getBuffer();
    Record record;
    for (nautilus::static_val<uint64_t> i = 0; i < schema.getFieldCount(); ++i)
    {
        const auto& fieldName = schema.getFieldByIndex(i).getName();
        if (!includesField(projections, fieldName))
        {
            continue;
        }
        auto fieldAddress = calculateFieldAddress(bufferAddress, recordIndex, i);
        auto value = loadValue(columnMemoryLayout->getPhysicalType(i), recordBuffer, fieldAddress);
        record.write(fieldName, value);
    }
    return record;
}

void ColumnTupleBufferMemoryProvider::writeRecord(
    nautilus::val<uint64_t>& recordIndex,
    const RecordBuffer& recordBuffer,
    const Record& rec,
    const nautilus::val<Memory::AbstractBufferProvider*>& bufferProvider) const
{
    const auto& schema = columnMemoryLayout->getSchema();
    const auto bufferAddress = recordBuffer.getBuffer();
    for (nautilus::static_val<size_t> i = 0; i < schema.getFieldCount(); ++i)
    {
        auto fieldAddress = calculateFieldAddress(bufferAddress, recordIndex, i);
        const auto value = rec.read(schema.getFieldByIndex(i).getName());
        storeValue(columnMemoryLayout->getPhysicalType(i), recordBuffer, fieldAddress, value, bufferProvider);
    }
}

}
