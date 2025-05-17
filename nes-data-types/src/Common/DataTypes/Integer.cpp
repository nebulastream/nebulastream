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
#include <utility>
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
        return bits == otherInteger->bits && isSigned == otherInteger->getIsSigned();
    }
    return false;
}

std::shared_ptr<DataType> Integer::join(const DataType& otherDataType) const
{
    if (dynamic_cast<const Numeric*>(&otherDataType) == nullptr)
    {
        return std::make_shared<Undefined>();
    }

    if (auto newDataType = inferDataType(*this, *dynamic_cast<const Numeric*>(&otherDataType)); newDataType.has_value())
    {
        return std::move(newDataType.value());
    }
    throw DifferentFieldTypeExpected("Cannot join {} and {}", toString(), otherDataType.toString());
}

std::string Integer::toString() const
{
    return fmt::format("{}{}", isSigned ? "INT" : "UINT", std::to_string(bits));
}

bool Integer::getIsSigned() const
{
    return isSigned;
}

DataTypeRegistryReturnType DataTypeGeneratedRegistrar::RegisterINT8DataType(DataTypeRegistryArguments)
{
    return std::make_unique<Integer>(8, true);
}

DataTypeRegistryReturnType DataTypeGeneratedRegistrar::RegisterUINT8DataType(DataTypeRegistryArguments)
{
    return std::make_unique<Integer>(8, false);
}

DataTypeRegistryReturnType DataTypeGeneratedRegistrar::RegisterINT16DataType(DataTypeRegistryArguments)
{
    return std::make_unique<Integer>(16, true);
}

DataTypeRegistryReturnType DataTypeGeneratedRegistrar::RegisterUINT16DataType(DataTypeRegistryArguments)
{
    return std::make_unique<Integer>(16, false);
}

DataTypeRegistryReturnType DataTypeGeneratedRegistrar::RegisterINT32DataType(DataTypeRegistryArguments)
{
    return std::make_unique<Integer>(32, true);
}

DataTypeRegistryReturnType DataTypeGeneratedRegistrar::RegisterUINT32DataType(DataTypeRegistryArguments)
{
    return std::make_unique<Integer>(32, false);
}

DataTypeRegistryReturnType DataTypeGeneratedRegistrar::RegisterINT64DataType(DataTypeRegistryArguments)
{
    return std::make_unique<Integer>(64, true);
}

DataTypeRegistryReturnType DataTypeGeneratedRegistrar::RegisterUINT64DataType(DataTypeRegistryArguments)
{
    return std::make_unique<Integer>(64, false);
}

}
