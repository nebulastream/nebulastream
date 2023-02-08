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
#include <Common/PhysicalTypes/PhysicalType.hpp>
#include <Nautilus/Interface/DataTypes/Utils.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <cstring>
namespace NES::Nautilus {

Value<> loadValue(Value<MemRef>& fieldReference, const PhysicalTypePtr& dataType) {
    if (dataType->isBasicType()) {
        auto basicType = std::static_pointer_cast<BasicPhysicalType>(dataType);
        switch (basicType->nativeType) {
            case BasicPhysicalType::BOOLEAN: {
                return fieldReference.load<Nautilus::Boolean>();
            };
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
            default: {
                NES_ERROR("load for Physical Type: " << dataType << " is currently not supported");
                NES_NOT_IMPLEMENTED();
            };
        }
    }
    NES_NOT_IMPLEMENTED();
}

bool memeq(void* ptr1, void* ptr2, uint64_t size) { return memcmp(ptr1, ptr2, size) == 0; }

bool memEquals(Value<MemRef>&& ptr1, Value<MemRef>&& ptr2, Value<UInt64>&& size) {
    return FunctionCall("memeq", memeq, ptr1, ptr2, size);
}
void memCopy(Value<MemRef>&& ptr1, Value<MemRef>&& ptr2, Value<UInt64>&& size) {
    FunctionCall("memcpy", memcpy, ptr1, ptr2, size);
}
}// namespace NES::Nautilus