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

#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <Execution/MemoryProvider/RowTupleBufferMemoryProvider.hpp>
#include <MemoryLayout/MemoryLayout.hpp>
#include <MemoryLayout/RowLayout.hpp>
#include <nautilus/val_ptr.hpp>

namespace NES::Runtime::Execution::MemoryProvider
{

RowTupleBufferMemoryProvider::RowTupleBufferMemoryProvider(std::shared_ptr<Memory::MemoryLayouts::RowLayout> rowMemoryLayoutPtr)
    : rowMemoryLayoutPtr(rowMemoryLayoutPtr) {};

Memory::MemoryLayouts::MemoryLayoutPtr RowTupleBufferMemoryProvider::getMemoryLayoutPtr()
{
    return rowMemoryLayoutPtr;
}

nautilus::val<int8_t*>
RowTupleBufferMemoryProvider::calculateFieldAddress(const nautilus::val<int8_t*>& recordOffset, const uint64_t fieldIndex) const
{
    auto& fieldOffset = rowMemoryLayoutPtr->getFieldOffSets()[fieldIndex];
    auto fieldAddress = recordOffset + nautilus::val<uint64_t>(fieldOffset);
    return fieldAddress;
}

Nautilus::Record RowTupleBufferMemoryProvider::readRecord(
    const std::vector<Nautilus::Record::RecordFieldIdentifier>& projections,
    nautilus::val<int8_t*>& bufferAddress,
    nautilus::val<uint64_t>& recordIndex) const
{
    /// read all fields
    auto& schema = rowMemoryLayoutPtr->getSchema();
    Nautilus::Record record;
    const auto tupleSize = rowMemoryLayoutPtr->getTupleSize();
    const auto recordOffset = bufferAddress + (tupleSize * recordIndex);
    for (nautilus::static_val<uint64_t> i = 0; i < schema->getSize(); ++i)
    {
        auto& fieldName = schema->fields[i]->getName();
        if (!includesField(projections, fieldName))
        {
            continue;
        }
        auto fieldAddress = calculateFieldAddress(recordOffset, i);
        auto value = loadValue(rowMemoryLayoutPtr->getPhysicalTypes()[i], bufferAddress, fieldAddress);
        record.write(rowMemoryLayoutPtr->getSchema()->fields[i]->getName(), value);
    }
    return record;
}

void RowTupleBufferMemoryProvider::writeRecord(
    nautilus::val<uint64_t>& recordIndex, nautilus::val<int8_t*>& bufferAddress, NES::Nautilus::Record& rec) const
{
    auto fieldSizes = rowMemoryLayoutPtr->getFieldSizes();
    auto tupleSize = rowMemoryLayoutPtr->getTupleSize();
    auto recordOffset = bufferAddress + (tupleSize * recordIndex);
    auto schema = rowMemoryLayoutPtr->getSchema();
    for (nautilus::static_val<size_t> i = 0; i < fieldSizes.size(); ++i)
    {
        auto fieldAddress = calculateFieldAddress(recordOffset, i);
        const auto& value = rec.read(schema->fields[i]->getName());
        storeValue(rowMemoryLayoutPtr->getPhysicalTypes()[i], bufferAddress, fieldAddress, value);
    }
}

}
