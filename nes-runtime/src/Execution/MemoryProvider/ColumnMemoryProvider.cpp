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
#include <Execution/MemoryProvider/ColumnMemoryProvider.hpp>
#include <Runtime/MemoryLayout/ColumnLayout.hpp>

namespace NES::Runtime::Execution::MemoryProvider {

ColumnMemoryProvider::ColumnMemoryProvider(Runtime::MemoryLayouts::ColumnLayoutPtr columnMemoryLayoutPtr) : 
    columnMemoryLayoutPtr(columnMemoryLayoutPtr) {};

MemoryLayouts::MemoryLayoutPtr ColumnMemoryProvider::getMemoryLayoutPtr() {
    return columnMemoryLayoutPtr;
}

Nautilus::Record ColumnMemoryProvider::read(const std::vector<Nautilus::Record::RecordFieldIdentifier>& projections,
                        Nautilus::Value<Nautilus::MemRef> bufferAddress,
                        Nautilus::Value<Nautilus::UInt64> recordIndex) {
    // read all fields
    Nautilus::Record record;
    for (uint64_t i = 0; i < columnMemoryLayoutPtr->getSchema()->getSize(); i++) {
        auto fieldName = columnMemoryLayoutPtr->getSchema()->fields[i]->getName();
        if (!includesField(projections, fieldName)) {
            continue;
        }
        auto fieldSize = columnMemoryLayoutPtr->getFieldSizes()[i];
        auto columnOffset = columnMemoryLayoutPtr->getColumnOffsets()[i];
        auto fieldOffset = recordIndex * fieldSize + columnOffset;
        auto fieldAddress = bufferAddress + fieldOffset;
        auto memRef = fieldAddress.as<Nautilus::MemRef>();
        auto value = load(columnMemoryLayoutPtr->getPhysicalTypes()[i], memRef);
        record.write(columnMemoryLayoutPtr->getSchema()->fields[i]->getName(), value);
    }
    return record;
}

void ColumnMemoryProvider::write(Nautilus::Value<NES::Nautilus::UInt64> recordIndex, 
                                 Nautilus::Value<Nautilus::MemRef> bufferAddress, 
                                 NES::Nautilus::Record& rec) {
    auto fieldSizes = columnMemoryLayoutPtr->getFieldSizes();
    auto schema = columnMemoryLayoutPtr->getSchema();
    auto& columnOffsets = columnMemoryLayoutPtr->getColumnOffsets();
    for (uint64_t i = 0; i < fieldSizes.size(); i++) {
        auto fieldOffset = (recordIndex * fieldSizes[i]) + columnOffsets[i];
        auto fieldAddress = bufferAddress + fieldOffset;
        auto value = rec.read(schema->fields[i]->getName());
        auto memRef = fieldAddress.as<Nautilus::MemRef>();
        memRef.store(value);
    }
}

} //namespace