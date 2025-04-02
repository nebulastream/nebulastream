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

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <memory>
#include <string>
#include <Util/Common.hpp>
#include <fmt/format.h>
#include <DataTypeRegistry.hpp>
#include <Common/DataTypes/Float.hpp>
#include <Common/DataTypes/Integer.hpp>
#include <Common/DataTypes/Numeric.hpp>
#include <Common/DataTypes/Undefined.hpp>

#include <ErrorHandling.hpp>

namespace NES
{

bool Integer::operator==(const NES::DataType& other) const
{
    if (const auto otherInteger = dynamic_cast<const Integer*>(&other))
    {
        return bits == otherInteger->bits && isSigned == otherInteger->getIsSigned() && nullable == other.nullable;
    }
    return false;
}

std::shared_ptr<DataType> Integer::join(const std::shared_ptr<DataType> otherDataType)
{
    if (NES::Util::instanceOf<Undefined>(otherDataType))
    {
        return std::make_shared<Integer>(nullable || otherDataType->nullable, bits, isSigned);
    }

    if (not NES::Util::instanceOf<Numeric>(otherDataType))
    {
        throw DifferentFieldTypeExpected("Cannot join {} and {}", toString(), otherDataType->toString());
    }

    if (const auto newDataType = Numeric::inferDataType(*this, *NES::Util::as<Numeric>(otherDataType)); newDataType.has_value())
    {
        return newDataType.value();
    }
    throw DifferentFieldTypeExpected("Cannot join {} and {}", toString(), otherDataType->toString());
}

std::string Integer::toString()
{
    return fmt::format("{}{}", isSigned ? "INT" : "UINT", std::to_string(bits));
}

bool Integer::getIsSigned() const
{
    return isSigned;
}

DataTypeRegistryReturnType DataTypeGeneratedRegistrar::RegisterINT8DataType(DataTypeRegistryArguments args)
{
    return std::make_shared<Integer>(args.nullable, 8, INT8_MIN, INT8_MAX);
}

DataTypeRegistryReturnType DataTypeGeneratedRegistrar::RegisterUINT8DataType(DataTypeRegistryArguments args)
{
    return std::make_shared<Integer>(args.nullable, 8, 0, UINT8_MAX);
}

DataTypeRegistryReturnType DataTypeGeneratedRegistrar::RegisterINT16DataType(DataTypeRegistryArguments args)
{
    return std::make_shared<Integer>(args.nullable, 16, INT16_MIN, INT16_MAX);
}

DataTypeRegistryReturnType DataTypeGeneratedRegistrar::RegisterUINT16DataType(DataTypeRegistryArguments args)
{
    return std::make_shared<Integer>(args.nullable, 16, 0, UINT16_MAX);
}

DataTypeRegistryReturnType DataTypeGeneratedRegistrar::RegisterINT32DataType(DataTypeRegistryArguments args)
{
    return std::make_shared<Integer>(args.nullable, 32, INT32_MIN, INT32_MAX);
}

DataTypeRegistryReturnType DataTypeGeneratedRegistrar::RegisterUINT32DataType(DataTypeRegistryArguments args)
{
    return std::make_shared<Integer>(args.nullable, 32, 0, UINT32_MAX);
}

DataTypeRegistryReturnType DataTypeGeneratedRegistrar::RegisterINT64DataType(DataTypeRegistryArguments args)
{
    return std::make_shared<Integer>(args.nullable, 64, INT64_MIN, INT64_MAX);
}

DataTypeRegistryReturnType DataTypeGeneratedRegistrar::RegisterUINT64DataType(DataTypeRegistryArguments args)
{
    return std::make_shared<Integer>(args.nullable, 64, 0, UINT64_MAX);
}

}
