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
        return bits == otherInteger->bits && lowerBound == otherInteger->lowerBound && upperBound == otherInteger->upperBound;
    }
    return false;
}

std::shared_ptr<DataType> Integer::join(std::shared_ptr<DataType> otherDataType) const
{
    if (dynamic_cast<const Undefined*>(otherDataType.get()) != nullptr)
    {
        return std::make_unique<Integer>(bits, lowerBound, upperBound);
    }
    if (dynamic_cast<const Numeric*>(otherDataType.get()) == nullptr)
    {
        throw DifferentFieldTypeExpected("Cannot join {} and {}", toString(), otherDataType.get()->toString());
    }

    if (auto newDataType = inferDataType(*this, *dynamic_cast<const Numeric*>(otherDataType.get())); newDataType.has_value())
    {
        return std::move(newDataType.value());
    }
    throw DifferentFieldTypeExpected("Cannot join {} and {}", toString(), otherDataType.get()->toString());
}

std::string Integer::toString() const
{
    return fmt::format("{}{}", lowerBound == 0 ? "UINT" : "INT", std::to_string(bits));
}

DataTypeRegistryReturnType DataTypeGeneratedRegistrar::RegisterINT8DataType(DataTypeRegistryArguments)
{
    return std::make_unique<Integer>(8, INT8_MIN, INT8_MAX);
}

DataTypeRegistryReturnType DataTypeGeneratedRegistrar::RegisterUINT8DataType(DataTypeRegistryArguments)
{
    return std::make_unique<Integer>(8, 0, UINT8_MAX);
}

DataTypeRegistryReturnType DataTypeGeneratedRegistrar::RegisterINT16DataType(DataTypeRegistryArguments)
{
    return std::make_unique<Integer>(16, INT16_MIN, INT16_MAX);
}

DataTypeRegistryReturnType DataTypeGeneratedRegistrar::RegisterUINT16DataType(DataTypeRegistryArguments)
{
    return std::make_unique<Integer>(16, 0, UINT16_MAX);
}

DataTypeRegistryReturnType DataTypeGeneratedRegistrar::RegisterINT32DataType(DataTypeRegistryArguments)
{
    return std::make_unique<Integer>(32, INT32_MIN, INT32_MAX);
}

DataTypeRegistryReturnType DataTypeGeneratedRegistrar::RegisterUINT32DataType(DataTypeRegistryArguments)
{
    return std::make_unique<Integer>(32, 0, UINT32_MAX);
}

DataTypeRegistryReturnType DataTypeGeneratedRegistrar::RegisterINT64DataType(DataTypeRegistryArguments)
{
    return std::make_unique<Integer>(64, INT64_MIN, INT64_MAX);
}

DataTypeRegistryReturnType DataTypeGeneratedRegistrar::RegisterUINT64DataType(DataTypeRegistryArguments)
{
    return std::make_unique<Integer>(64, 0, UINT64_MAX);
}

}
