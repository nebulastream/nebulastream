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

#include <cmath>
#include <limits>
#include <memory>
#include <string>

#include <fmt/format.h>

#include <Util/Common.hpp>
#include <DataTypeRegistry.hpp>
#include <ErrorHandling.hpp>
#include <Common/DataTypes/Float.hpp>
#include <Common/DataTypes/Numeric.hpp>
#include <Common/DataTypes/Undefined.hpp>

namespace NES
{

bool Float::operator==(const NES::DataType& other) const
{
    if (const auto otherFloat = dynamic_cast<const Float*>(&other))
    {
        return bits == otherFloat->bits && lowerBound == otherFloat->lowerBound && upperBound == otherFloat->upperBound
            && nullable == other.nullable;
    }
    return false;
}

std::shared_ptr<DataType> Float::join(const std::shared_ptr<DataType> otherDataType)
{
    if (NES::Util::instanceOf<Undefined>(otherDataType))
    {
        return std::make_shared<Float>(nullable || otherDataType->nullable, bits, lowerBound, upperBound);
    }
    if (not NES::Util::instanceOf<Numeric>(otherDataType))
    {
        throw DifferentFieldTypeExpected("Cannot join {} and {}", toString(), otherDataType->toString());
    }

    if (const auto newDataType = inferDataType(*this, *NES::Util::as<Numeric>(otherDataType)); newDataType.has_value())
    {
        return newDataType.value();
    }
    throw DifferentFieldTypeExpected("Cannot join {} and {}", toString(), otherDataType->toString());
}

std::string Float::toString()
{
    return fmt::format("FLOAT{}", bits);
}

DataTypeRegistryReturnType DataTypeGeneratedRegistrar::RegisterFLOAT32DataType(DataTypeRegistryArguments args)
{
    return std::make_shared<Float>(args.nullable, 32, std::numeric_limits<float>::lowest(), std::numeric_limits<float>::max());
}

DataTypeRegistryReturnType DataTypeGeneratedRegistrar::RegisterFLOAT64DataType(DataTypeRegistryArguments args)
{
    return std::make_shared<Float>(args.nullable, 64, std::numeric_limits<double>::lowest(), std::numeric_limits<double>::max());
}

}
