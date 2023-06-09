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
#include <Execution/MemoryProvider/RowMemoryProvider.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>

namespace NES::Runtime::Execution::MemoryProvider {

RowMemoryProvider::RowMemoryProvider(const Runtime::MemoryLayouts::RowLayoutPtr& rowMemoryLayoutPtr,
                                     const std::vector<Nautilus::Record::RecordFieldIdentifier>& projections)
    : MemoryProvider(rowMemoryLayoutPtr, projections), rowMemoryLayoutPtr(rowMemoryLayoutPtr){};

MemoryLayouts::MemoryLayoutPtr RowMemoryProvider::getMemoryLayoutPtr() { return rowMemoryLayoutPtr; }

Nautilus::Value<Nautilus::MemRef> RowMemoryProvider::calculateFieldAddress(Nautilus::Value<>& recordOffset,
                                                                           uint64_t fieldIndex) const {
    auto fieldOffset = rowMemoryLayoutPtr->getFieldOffSets()[fieldIndex];
    auto fieldAddress = recordOffset + fieldOffset;
    return fieldAddress.as<Nautilus::MemRef>();
}

Nautilus::Record RowMemoryProvider::read(Nautilus::Value<Nautilus::MemRef>& bufferAddress,
                                         Nautilus::Value<Nautilus::UInt64>& recordIndex) const {
    // read all fields
    auto rowLayout = std::dynamic_pointer_cast<Runtime::MemoryLayouts::RowLayout>(rowMemoryLayoutPtr);
    auto tupleSize = rowMemoryLayoutPtr->getTupleSize();
    auto recordOffset = bufferAddress + (tupleSize * recordIndex);
    Nautilus::Record record;
    for (auto [fieldIndex, fieldName] : fields) {
        auto fieldAddress = calculateFieldAddress(recordOffset, fieldIndex);
        auto value = load(rowMemoryLayoutPtr->getPhysicalTypes()[fieldIndex], bufferAddress, fieldAddress);
        record.write(fieldName, value);
    }
    return record;
}

void RowMemoryProvider::write(Nautilus::Value<NES::Nautilus::UInt64>& recordIndex,
                              Nautilus::Value<Nautilus::MemRef>& bufferAddress,
                              NES::Nautilus::Record& rec) const {
    auto tupleSize = rowMemoryLayoutPtr->getTupleSize();
    auto recordOffset = bufferAddress + (tupleSize * recordIndex);
    auto schema = rowMemoryLayoutPtr->getSchema();
    for (auto [fieldIndex, fieldName] : fields) {
        auto fieldOffset = rowMemoryLayoutPtr->getFieldOffSets()[fieldIndex];
        auto fieldAddress = calculateFieldAddress(recordOffset, fieldIndex);
        auto value = rec.read(fieldName);
        store(rowMemoryLayoutPtr->getPhysicalTypes()[fieldIndex], bufferAddress, fieldAddress, value);
    }
}

}// namespace NES::Runtime::Execution::MemoryProvider