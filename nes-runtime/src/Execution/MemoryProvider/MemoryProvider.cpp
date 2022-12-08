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

#include <Common/PhysicalTypes/BasicPhysicalType.hpp>
#include <Execution/MemoryProvider/MemoryProvider.hpp>
#include <Nautilus/Interface/DataTypes/Text/Text.hpp>
#include <Nautilus/Interface/DataTypes/Text/TextValue.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/LocalBufferPool.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/WorkerContext.hpp>

namespace NES::Runtime::Execution::MemoryProvider {

Nautilus::TextValue* loadAssociatedTextValue(void* tupleBuffer, uint32_t childIndex) {
    auto tb = TupleBuffer::reinterpretAsTupleBuffer(tupleBuffer);
    auto childBuffer = tb.loadChildBuffer(childIndex);
    return Nautilus::TextValue::load(childBuffer);
}

Nautilus::Value<> MemoryProvider::load(const PhysicalTypePtr& type,
                                       Nautilus::Value<Nautilus::MemRef>& bufferReference,
                                       Nautilus::Value<Nautilus::MemRef>& fieldReference) const {
    if (type->isBasicType()) {
        auto basicType = std::static_pointer_cast<BasicPhysicalType>(type);
        switch (basicType->nativeType) {
            case BasicPhysicalType::INT_8: {
                return fieldReference.load<Nautilus::Int8>();
            };
            case BasicPhysicalType::INT_16: {
                return fieldReference.load<Nautilus::Int16>();
            };
            case BasicPhysicalType::INT_32: {
                return fieldReference.load<Nautilus::Int32>();
            };
            case BasicPhysicalType::INT_64: {
                return fieldReference.load<Nautilus::Int64>();
            };
            case BasicPhysicalType::UINT_8: {
                return fieldReference.load<Nautilus::UInt8>();
            };
            case BasicPhysicalType::UINT_16: {
                return fieldReference.load<Nautilus::UInt16>();
            };
            case BasicPhysicalType::UINT_32: {
                return fieldReference.load<Nautilus::UInt32>();
            };
            case BasicPhysicalType::UINT_64: {
                return fieldReference.load<Nautilus::UInt64>();
            };
            case BasicPhysicalType::FLOAT: {
                return fieldReference.load<Nautilus::Float>();
            };
            case BasicPhysicalType::DOUBLE: {
                return fieldReference.load<Nautilus::Double>();
            };
            case BasicPhysicalType::TEXT: {
                auto childIndex = fieldReference.load<Nautilus::UInt32>();
                auto variableSizeBuffer =
                    Nautilus::FunctionCall("loadAssociatedTextValue", loadAssociatedTextValue, bufferReference, childIndex);
                return variableSizeBuffer;
            };
            default: {
                NES_ERROR("MemoryProvider::load: Physical Type: " << type << " is currently not supported");
                NES_NOT_IMPLEMENTED();
            };
        }
    }
    NES_NOT_IMPLEMENTED();
}

uint32_t storeAssociatedTextValue(void* tupleBuffer, const Nautilus::TextValue* textValue) {
    auto tb = TupleBuffer::reinterpretAsTupleBuffer(tupleBuffer);
    auto textBuffer = TupleBuffer::reinterpretAsTupleBuffer((void*) textValue);
    return tb.storeChildBuffer(textBuffer);
}

Nautilus::Value<> MemoryProvider::store(const NES::PhysicalTypePtr& type,
                                        Nautilus::Value<Nautilus::MemRef>& bufferReference,
                                        Nautilus::Value<Nautilus::MemRef>& fieldReference,
                                        Nautilus::Value<>& value) const {
    if (type->isBasicType()) {
        auto basicType = std::static_pointer_cast<BasicPhysicalType>(type);
        switch (basicType->nativeType) {
            case BasicPhysicalType::TEXT: {
                auto textValue = value.as<Nautilus::Text>();
                auto childIndex = Nautilus::FunctionCall("storeAssociatedTextValue",
                                                         storeAssociatedTextValue,
                                                         bufferReference,
                                                         textValue->getReference());
                fieldReference.store(childIndex);
                return value;
            };
            default: {
                fieldReference.store(value);
                return value;
            };
        }
    }
    NES_NOT_IMPLEMENTED();
}

bool MemoryProvider::includesField(const std::vector<Nautilus::Record::RecordFieldIdentifier>& projections,
                                   Nautilus::Record::RecordFieldIdentifier fieldIndex) const {
    if (projections.empty()) {
        return true;
    }
    return std::find(projections.begin(), projections.end(), fieldIndex) != projections.end();
}

MemoryProvider::~MemoryProvider() {}

}// namespace NES::Runtime::Execution::MemoryProvider