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

#include <API/Schema.hpp>
#include <Common/PhysicalTypes/BasicPhysicalType.hpp>
#include <Execution/MemoryProvider/ColumnTupleBufferMemoryProvider.hpp>
#include <Execution/MemoryProvider/RowTupleBufferMemoryProvider.hpp>
#include <Execution/MemoryProvider/TupleBufferMemoryProvider.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <Runtime/MemoryLayout/ColumnLayout.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Logger.hpp>
#include <cstdint>
#include <nautilus/function.hpp>
#include <nautilus/val_ptr.hpp>
#include <string>

namespace NES::Runtime::Execution::MemoryProvider {

const uint8_t* loadAssociatedTextValue(void* tupleBuffer, uint32_t childIndex) {
    auto tb = TupleBuffer::reinterpretAsTupleBuffer(tupleBuffer);
    auto childBuffer = tb.loadChildBuffer(childIndex);
    return childBuffer.getBuffer<uint8_t>();
}

Nautilus::VarVal TupleBufferMemoryProvider::load(const PhysicalTypePtr& type,
                                                 const Nautilus::MemRefVal& bufferReference,
                                                       Nautilus::MemRefVal& fieldReference) {
    if (type->isBasicType()) {
        return Nautilus::readVarValFromMemRef(fieldReference, type);
    } else if (type->isTextType()) {
        const auto childIndex = readValueFromMemRef(fieldReference, uint32_t);
        const auto textPtr = nautilus::invoke(loadAssociatedTextValue, bufferReference, childIndex);
        return Nautilus::VariableSizedData(textPtr);
    } else {
        NES_ERROR("Physical Type: type {} is currently not supported", type->toString());
        NES_NOT_IMPLEMENTED();
    }
}

uint32_t storeAssociatedTextValue(void* tupleBuffer, const int8_t* textValue) {
    auto tb = TupleBuffer::reinterpretAsTupleBuffer(tupleBuffer);
    auto textBuffer = TupleBuffer::reinterpretAsTupleBuffer((void*)textValue);
    return tb.storeChildBuffer(textBuffer);
}

Nautilus::VarVal TupleBufferMemoryProvider::store(const NES::PhysicalTypePtr& type,
                                                  const Nautilus::MemRefVal& bufferReference,
                                                        Nautilus::MemRefVal& fieldReference,
                                                        Nautilus::VarVal value) {
    if (type->isBasicType()) {
        value.writeToMemRefVal(fieldReference);
        return value;
    } else if (type->isTextType()) {
        auto textValue = value.cast<Nautilus::VariableSizedData>();
        auto childIndex = nautilus::invoke(storeAssociatedTextValue, bufferReference, textValue.getReference());
        auto fieldReferenceCastedU32 = static_cast<nautilus::val<uint32_t*>>(fieldReference);
        *fieldReferenceCastedU32 = childIndex;
        return value;
        NES_NOT_IMPLEMENTED();
    }
    NES_NOT_IMPLEMENTED();
}




bool TupleBufferMemoryProvider::includesField(const std::vector<Nautilus::Record::RecordFieldIdentifier>& projections,
                                              const Nautilus::Record::RecordFieldIdentifier& fieldIndex) {
    if (projections.empty()) {
        return true;
    }
    return std::find(projections.begin(), projections.end(), fieldIndex) != projections.end();
}

TupleBufferMemoryProvider::~TupleBufferMemoryProvider() = default;

MemoryProviderPtr TupleBufferMemoryProvider::createMemoryProvider(const uint64_t bufferSize, const SchemaPtr schema) {
    if (schema->getLayoutType() == Schema::MemoryLayoutType::ROW_LAYOUT) {
        auto rowMemoryLayout = MemoryLayouts::RowLayout::create(schema, bufferSize);
        return std::make_unique<Runtime::Execution::MemoryProvider::RowTupleBufferMemoryProvider>(rowMemoryLayout);
    } else if (schema->getLayoutType() == Schema::MemoryLayoutType::COLUMNAR_LAYOUT) {
        auto columnMemoryLayout = MemoryLayouts::ColumnLayout::create(schema, bufferSize);
        return std::make_unique<Runtime::Execution::MemoryProvider::ColumnTupleBufferMemoryProvider>(columnMemoryLayout);
    } else {
        NES_NOT_IMPLEMENTED();
    }
}

}// namespace NES::Runtime::Execution::MemoryProvider
