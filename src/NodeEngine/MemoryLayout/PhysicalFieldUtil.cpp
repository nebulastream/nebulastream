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

#include <Common/DataTypes/Array.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/PhysicalTypes/ArrayPhysicalType.hpp>
#include <Common/PhysicalTypes/BasicPhysicalType.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>
#include <NodeEngine/MemoryLayout/BasicPhysicalField.hpp>
#include <NodeEngine/MemoryLayout/PhysicalField.hpp>
#include <NodeEngine/MemoryLayout/PhysicalFieldUtil.hpp>
#include <Util/Logger.hpp>

namespace NES {

class ArrayDataType;

std::shared_ptr<PhysicalField> PhysicalFieldUtil::createPhysicalField(const PhysicalTypePtr physicalType,
                                                                      uint64_t bufferOffset) {
    if (physicalType->isBasicType()) {
        auto basicPhysicalType = std::dynamic_pointer_cast<BasicPhysicalType>(physicalType);
        switch (basicPhysicalType->getNativeType()) {
            case BasicPhysicalType::INT_8: return createBasicPhysicalField<int8_t>(bufferOffset);
            case BasicPhysicalType::INT_16: return createBasicPhysicalField<int16_t>(bufferOffset);
            case BasicPhysicalType::INT_32: return createBasicPhysicalField<int32_t>(bufferOffset);
            case BasicPhysicalType::INT_64: return createBasicPhysicalField<int64_t>(bufferOffset);
            case BasicPhysicalType::UINT_8: return createBasicPhysicalField<uint8_t>(bufferOffset);
            case BasicPhysicalType::UINT_16: return createBasicPhysicalField<uint16_t>(bufferOffset);
            case BasicPhysicalType::UINT_32: return createBasicPhysicalField<uint32_t>(bufferOffset);
            case BasicPhysicalType::UINT_64: return createBasicPhysicalField<uint64_t>(bufferOffset);
            case BasicPhysicalType::FLOAT: return createBasicPhysicalField<float>(bufferOffset);
            case BasicPhysicalType::DOUBLE: return createBasicPhysicalField<double>(bufferOffset);
            case BasicPhysicalType::CHAR: return createBasicPhysicalField<char>(bufferOffset);
            case BasicPhysicalType::BOOLEAN: return createBasicPhysicalField<bool>(bufferOffset);
        }
    } else if (physicalType->isArrayType()) {
        auto arrayPhysicalType = std::dynamic_pointer_cast<ArrayPhysicalType>(physicalType);
        return createArrayPhysicalField(arrayPhysicalType->getPhysicalComponentType(), bufferOffset);
    } else {
        // TODO FIXME
        NES_FATAL_ERROR("No physical field mapping for test available");
        NES_NOT_IMPLEMENTED();
    }
    return nullptr;
}
}// namespace NES