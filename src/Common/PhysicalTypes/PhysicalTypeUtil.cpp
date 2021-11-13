/*

     Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#include <Common/PhysicalTypes/ArrayPhysicalType.hpp>
#include <Common/PhysicalTypes/BasicPhysicalType.hpp>
#include <Common/PhysicalTypes/PhysicalTypeUtil.hpp>

namespace NES::PhysicalTypes {
bool isChar(PhysicalTypePtr physicalType) {
    return physicalType->isBasicType()
        && std::dynamic_pointer_cast<BasicPhysicalType>(physicalType)->nativeType == BasicPhysicalType::CHAR;
}
bool isUInt8(PhysicalTypePtr physicalType) {
    return physicalType->isBasicType()
        && std::dynamic_pointer_cast<BasicPhysicalType>(physicalType)->nativeType == BasicPhysicalType::UINT_8;
}
bool isUInt16(PhysicalTypePtr physicalType) {
    return physicalType->isBasicType()
        && std::dynamic_pointer_cast<BasicPhysicalType>(physicalType)->nativeType == BasicPhysicalType::UINT_16;
}
bool isUInt32(PhysicalTypePtr physicalType) {
    return physicalType->isBasicType()
        && std::dynamic_pointer_cast<BasicPhysicalType>(physicalType)->nativeType == BasicPhysicalType::UINT_32;
}
bool isUInt64(PhysicalTypePtr physicalType) {
    return physicalType->isBasicType()
        && std::dynamic_pointer_cast<BasicPhysicalType>(physicalType)->nativeType == BasicPhysicalType::UINT_64;
}
bool isInt8(PhysicalTypePtr physicalType) {
    return physicalType->isBasicType()
        && std::dynamic_pointer_cast<BasicPhysicalType>(physicalType)->nativeType == BasicPhysicalType::INT_8;
}
bool isInt16(PhysicalTypePtr physicalType) {
    return physicalType->isBasicType()
        && std::dynamic_pointer_cast<BasicPhysicalType>(physicalType)->nativeType == BasicPhysicalType::INT_16;
}
bool isInt32(PhysicalTypePtr physicalType) {
    return physicalType->isBasicType()
        && std::dynamic_pointer_cast<BasicPhysicalType>(physicalType)->nativeType == BasicPhysicalType::INT_32;
}
bool isInt64(PhysicalTypePtr physicalType) {
    return physicalType->isBasicType()
        && std::dynamic_pointer_cast<BasicPhysicalType>(physicalType)->nativeType == BasicPhysicalType::INT_64;
}
bool isFloat(PhysicalTypePtr physicalType) {
    return physicalType->isBasicType()
        && std::dynamic_pointer_cast<BasicPhysicalType>(physicalType)->nativeType == BasicPhysicalType::FLOAT;
}
bool isDouble(PhysicalTypePtr physicalType) {
    return physicalType->isBasicType()
        && std::dynamic_pointer_cast<BasicPhysicalType>(physicalType)->nativeType == BasicPhysicalType::DOUBLE;
}
bool isArray(PhysicalTypePtr physicalType) { return physicalType->isArrayType(); }
PhysicalTypePtr getArrayComponent(PhysicalTypePtr physicalType) {
    return std::dynamic_pointer_cast<ArrayPhysicalType>(physicalType)->physicalComponentType;
}

}// namespace NES::PhysicalTypes