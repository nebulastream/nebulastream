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

#include <cstdint>
#include <utility>
#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <Execution/MemoryProvider/ColumnTupleBufferMemoryProvider.hpp>
#include <MemoryLayout/ColumnLayout.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <val_ptr.hpp>

namespace NES::Runtime::Execution::MemoryProvider
{

ColumnTupleBufferMemoryProvider::ColumnTupleBufferMemoryProvider(std::shared_ptr<Memory::MemoryLayouts::ColumnLayout> columnMemoryLayoutPtr)
    : columnMemoryLayoutPtr(std::move(std::move(columnMemoryLayoutPtr))) {};

Memory::MemoryLayouts::MemoryLayoutPtr ColumnTupleBufferMemoryProvider::getMemoryLayoutPtr()
{
    return columnMemoryLayoutPtr;
}

nautilus::val<int8_t*> ColumnTupleBufferMemoryProvider::calculateFieldAddress(
    const nautilus::val<int8_t*>& bufferAddress, nautilus::val<uint64_t>& recordIndex, const uint64_t fieldIndex) const
{
    auto& fieldSize = columnMemoryLayoutPtr->getFieldSizes()[fieldIndex];
    auto& columnOffset = columnMemoryLayoutPtr->getColumnOffsets()[fieldIndex];
    const auto fieldOffset = recordIndex * fieldSize + columnOffset;
    auto fieldAddress = bufferAddress + fieldOffset;
    return fieldAddress;
}

Nautilus::Record ColumnTupleBufferMemoryProvider::readRecord(
    const std::vector<Nautilus::Record::RecordFieldIdentifier>& projections,
    const RecordBuffer& recordBuffer,
    nautilus::val<uint64_t>& recordIndex) const
{
    auto& schema = columnMemoryLayoutPtr->getSchema();
    /// read all fields
    const auto bufferAddress = recordBuffer.getBuffer();
    Nautilus::Record record;
    for (nautilus::static_val<uint64_t> i = 0; i < schema->getSize(); ++i)
    {
        auto& fieldName = schema->fields[i]->getName();
        if (!includesField(projections, fieldName))
        {
            continue;
        }
        auto fieldAddress = calculateFieldAddress(bufferAddress, recordIndex, i);
        auto value = loadValue(columnMemoryLayoutPtr->getPhysicalTypes()[i], recordBuffer, fieldAddress);
        record.write(fieldName, value);
    }
    return record;
}

void ColumnTupleBufferMemoryProvider::writeRecord(
    nautilus::val<uint64_t>& recordIndex, const RecordBuffer& recordBuffer, const Record& rec) const
{
    auto& fieldSizes = columnMemoryLayoutPtr->getFieldSizes();
    auto& schema = columnMemoryLayoutPtr->getSchema();
    const auto bufferAddress = recordBuffer.getBuffer();
    for (nautilus::static_val<size_t> i = 0; i < fieldSizes.size(); ++i)
    {
        auto fieldAddress = calculateFieldAddress(bufferAddress, recordIndex, i);
        const auto& value = rec.read(schema->fields[i]->getName());
        storeValue(columnMemoryLayoutPtr->getPhysicalTypes()[i], recordBuffer, fieldAddress, value);
    }
}

}
