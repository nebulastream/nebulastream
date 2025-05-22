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

#include <memory>
#include <Util/Common.hpp>
#include <Common/PhysicalTypes/BasicPhysicalType.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>
#include <Common/PhysicalTypes/PhysicalTypeUtil.hpp>
#include <Common/PhysicalTypes/VariableSizedDataPhysicalType.hpp>

namespace NES::PhysicalTypes
{
bool isSpecificBasicType(const PhysicalType& physicalType, BasicPhysicalType::NativeType specificBasicType)
{
    return ((dynamic_cast<const BasicPhysicalType*>(&physicalType)) != nullptr)
        && dynamic_cast<const BasicPhysicalType*>(&physicalType)->nativeType == specificBasicType;
}

bool isChar(const PhysicalType& physicalType)
{
    return isSpecificBasicType(physicalType, BasicPhysicalType::NativeType::CHAR);
}

bool isBool(const PhysicalType& physicalType)
{
    return isSpecificBasicType(physicalType, BasicPhysicalType::NativeType::BOOLEAN);
}

bool isUInt8(const PhysicalType& physicalType)
{
    return isSpecificBasicType(physicalType, BasicPhysicalType::NativeType::UINT_8);
}

bool isUInt16(const PhysicalType& physicalType)
{
    return isSpecificBasicType(physicalType, BasicPhysicalType::NativeType::UINT_16);
}

bool isUInt32(const PhysicalType& physicalType)
{
    return isSpecificBasicType(physicalType, BasicPhysicalType::NativeType::UINT_32);
}

bool isUInt64(const PhysicalType& physicalType)
{
    return isSpecificBasicType(physicalType, BasicPhysicalType::NativeType::UINT_64);
}

bool isInt8(const PhysicalType& physicalType)
{
    return isSpecificBasicType(physicalType, BasicPhysicalType::NativeType::INT_8);
}

bool isInt16(const PhysicalType& physicalType)
{
    return isSpecificBasicType(physicalType, BasicPhysicalType::NativeType::INT_16);
}

bool isInt32(const PhysicalType& physicalType)
{
    return isSpecificBasicType(physicalType, BasicPhysicalType::NativeType::INT_32);
}

bool isInt64(const PhysicalType& physicalType)
{
    return isSpecificBasicType(physicalType, BasicPhysicalType::NativeType::INT_64);
}

bool isFloat(const PhysicalType& physicalType)
{
    return isSpecificBasicType(physicalType, BasicPhysicalType::NativeType::FLOAT);
}

bool isDouble(const PhysicalType& physicalType)
{
    return isSpecificBasicType(physicalType, BasicPhysicalType::NativeType::DOUBLE);
}

bool isVariableSizedData(const std::shared_ptr<PhysicalType>& physicalType)
{
    return NES::Util::instanceOf<VariableSizedDataPhysicalType>(physicalType);
}

}
