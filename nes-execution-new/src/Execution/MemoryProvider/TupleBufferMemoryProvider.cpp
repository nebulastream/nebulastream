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
#include <Nautilus/DataTypes/ExecutableDataType.hpp>
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

Nautilus::ExecDataType TupleBufferMemoryProvider::load(const PhysicalTypePtr& type,
                                                       nautilus::val<int8_t*>& bufferReference,
                                                       nautilus::val<int8_t*>& fieldReference) {
    if (type->isBasicType()) {
        auto basicType = std::static_pointer_cast<BasicPhysicalType>(type);
        switch (basicType->nativeType) {
            case BasicPhysicalType::NativeType::BOOLEAN: {
                return Nautilus::ExecutableDataType<bool>::create(
                    static_cast<nautilus::val<bool>>(*static_cast<nautilus::val<bool*>>(fieldReference)));
            };
            case BasicPhysicalType::NativeType::INT_8: {
                return Nautilus::ExecutableDataType<int8_t>::create(
                    static_cast<nautilus::val<int8_t>>(*static_cast<nautilus::val<int8_t*>>(fieldReference)));
            };
            case BasicPhysicalType::NativeType::INT_16: {
                return Nautilus::ExecutableDataType<int16_t>::create(
                    static_cast<nautilus::val<int16_t>>(*static_cast<nautilus::val<int16_t*>>(fieldReference)));
            };
            case BasicPhysicalType::NativeType::INT_32: {
                return Nautilus::ExecutableDataType<int32_t>::create(
                    static_cast<nautilus::val<int32_t>>(*static_cast<nautilus::val<int32_t*>>(fieldReference)));
            };
            case BasicPhysicalType::NativeType::INT_64: {
                return Nautilus::ExecutableDataType<int64_t>::create(
                    static_cast<nautilus::val<int64_t>>(*static_cast<nautilus::val<int64_t*>>(fieldReference)));
            };
            case BasicPhysicalType::NativeType::UINT_8: {
                return Nautilus::ExecutableDataType<uint8_t>::create(
                    static_cast<nautilus::val<uint8_t>>(*static_cast<nautilus::val<uint8_t*>>(fieldReference)));
            };
            case BasicPhysicalType::NativeType::UINT_16: {
                return Nautilus::ExecutableDataType<uint16_t>::create(
                    static_cast<nautilus::val<uint16_t>>(*static_cast<nautilus::val<uint16_t*>>(fieldReference)));
            };
            case BasicPhysicalType::NativeType::UINT_32: {
                return Nautilus::ExecutableDataType<uint32_t>::create(
                    static_cast<nautilus::val<uint32_t>>(*static_cast<nautilus::val<uint32_t*>>(fieldReference)));
            };
            case BasicPhysicalType::NativeType::UINT_64: {
                return Nautilus::ExecutableDataType<uint64_t>::create(
                    static_cast<nautilus::val<uint64_t>>(*static_cast<nautilus::val<uint64_t*>>(fieldReference)));
            };
            case BasicPhysicalType::NativeType::FLOAT: {
                return Nautilus::ExecutableDataType<float>::create(
                    static_cast<nautilus::val<float>>(*static_cast<nautilus::val<float*>>(fieldReference)));
            };
            case BasicPhysicalType::NativeType::DOUBLE: {
                return Nautilus::ExecutableDataType<double>::create(
                    static_cast<nautilus::val<double>>(*static_cast<nautilus::val<double*>>(fieldReference)));
            };
            default: {
                NES_ERROR("Physical Type: {} is currently not supported", type->toString());
                NES_NOT_IMPLEMENTED();
            };
        }
    } else if (type->isArrayType()) {
        NES_ERROR("Physical Type: array type {} is currently not supported", type->toString());
        NES_NOT_IMPLEMENTED();
    } else if (type->isTextType()) {
        auto fieldReferenceCastedU32 = static_cast<nautilus::val<uint32_t*>>(fieldReference);
        nautilus::val<uint32_t> childIndex = *fieldReferenceCastedU32;
        auto textPtr = nautilus::invoke(loadAssociatedTextValue, bufferReference, childIndex);
        auto sizePtr = static_cast<nautilus::val<uint32_t*>>(fieldReference);
        auto contextPtr = textPtr + nautilus::val<uint64_t>(4);
        return Nautilus::ExecutableVariableDataType::create(contextPtr, sizePtr[0]);
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

Nautilus::ExecDataType TupleBufferMemoryProvider::store(const NES::PhysicalTypePtr& type,
                                                        nautilus::val<int8_t*>& bufferReference,
                                                        nautilus::val<int8_t*>& fieldReference,
                                                        Nautilus::ExecDataType value) {
    if (type->isBasicType()) {
        auto basicType = std::static_pointer_cast<BasicPhysicalType>(type);
        switch (basicType->nativeType) {
            case BasicPhysicalType::NativeType::BOOLEAN: {
                *static_cast<nautilus::val<bool*>>(fieldReference) =
                    std::dynamic_pointer_cast<Nautilus::ExecutableDataType<bool>>(value)->as<bool>();
                break;
            };
            case BasicPhysicalType::NativeType::INT_8: {
                *static_cast<nautilus::val<int8_t*>>(fieldReference) =
                    std::dynamic_pointer_cast<Nautilus::ExecutableDataType<int8_t>>(value)->as<int8_t>();
                break;
            };
            case BasicPhysicalType::NativeType::INT_16: {
                *static_cast<nautilus::val<int16_t*>>(fieldReference) =
                    std::dynamic_pointer_cast<Nautilus::ExecutableDataType<int16_t>>(value)->as<int16_t>();
                break;
            };
            case BasicPhysicalType::NativeType::INT_32: {
                *static_cast<nautilus::val<int32_t*>>(fieldReference) =
                    std::dynamic_pointer_cast<Nautilus::ExecutableDataType<int32_t>>(value)->as<int32_t>();
                break;
            };
            case BasicPhysicalType::NativeType::INT_64: {
                *static_cast<nautilus::val<int64_t*>>(fieldReference) =
                    std::dynamic_pointer_cast<Nautilus::ExecutableDataType<int64_t>>(value)->as<int64_t>();
                break;
            };
            case BasicPhysicalType::NativeType::UINT_8: {
                *static_cast<nautilus::val<uint8_t*>>(fieldReference) =
                    std::dynamic_pointer_cast<Nautilus::ExecutableDataType<uint8_t>>(value)->as<uint8_t>();
                break;
            };
            case BasicPhysicalType::NativeType::UINT_16: {
                *static_cast<nautilus::val<uint16_t*>>(fieldReference) =
                    std::dynamic_pointer_cast<Nautilus::ExecutableDataType<uint16_t>>(value)->as<uint16_t>();
                break;
            };
            case BasicPhysicalType::NativeType::UINT_32: {
                *static_cast<nautilus::val<uint32_t*>>(fieldReference) =
                    std::dynamic_pointer_cast<Nautilus::ExecutableDataType<uint32_t>>(value)->as<uint32_t>();
                break;
            };
            case BasicPhysicalType::NativeType::UINT_64: {
                *static_cast<nautilus::val<uint64_t*>>(fieldReference) =
                    std::dynamic_pointer_cast<Nautilus::ExecutableDataType<uint64_t>>(value)->as<uint64_t>();
                break;
            };
            case BasicPhysicalType::NativeType::FLOAT: {
                *static_cast<nautilus::val<float*>>(fieldReference) =
                    std::dynamic_pointer_cast<Nautilus::ExecutableDataType<float>>(value)->as<float>();
                break;
            };
            case BasicPhysicalType::NativeType::DOUBLE: {
                *static_cast<nautilus::val<double*>>(fieldReference) =
                    std::dynamic_pointer_cast<Nautilus::ExecutableDataType<double>>(value)->as<double>();
                break;
            };
            default: {
                NES_ERROR("Physical Type: {} is currently not supported", type->toString());
                NES_NOT_IMPLEMENTED();
            };
        }
        return value;
    } else if (type->isTextType()) {
        auto textValue = std::dynamic_pointer_cast<Nautilus::ExecutableVariableDataType>(value);
        auto childIndex = nautilus::invoke(storeAssociatedTextValue, bufferReference, textValue->getContent());
        auto fieldReferenceCastedU32 = static_cast<nautilus::val<uint32_t*>>(fieldReference);
        *fieldReferenceCastedU32 = childIndex;
        return value;
        NES_NOT_IMPLEMENTED();
    }
    NES_NOT_IMPLEMENTED();
}




bool TupleBufferMemoryProvider::includesField(const std::vector<Nautilus::Record::RecordFieldIdentifier>& projections,
                                              const Nautilus::Record::RecordFieldIdentifier& fieldIndex) const {
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
