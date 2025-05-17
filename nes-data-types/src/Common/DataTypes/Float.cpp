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
#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <Util/Common.hpp>
#include <fmt/format.h>
#include <DataTypeRegistry.hpp>
#include <ErrorHandling.hpp>
#include <Common/DataTypes/Float.hpp>
#include <Common/DataTypes/Integer.hpp>
#include <Common/DataTypes/Numeric.hpp>
#include <Common/DataTypes/Undefined.hpp>

namespace NES
{

bool Float::operator==(const NES::DataType& other) const
{
    if (const auto otherFloat = dynamic_cast<const Float*>(&other))
    {
        return bits == otherFloat->bits;
    }
    return false;
}

std::shared_ptr<DataType> Float::join(const DataType& otherDataType) const
{
    if (dynamic_cast<const Undefined*>(&otherDataType) != nullptr)
    {
        return std::make_unique<Undefined>();
    }
    if (dynamic_cast<const Numeric*>(&otherDataType) == nullptr)
    {
        throw DifferentFieldTypeExpected("Cannot join {} and {}", toString(), otherDataType.toString());
    }

    if (auto newDataType = inferDataType(*this, *dynamic_cast<const Numeric*>(&otherDataType)); newDataType.has_value())
    {
        return std::move(newDataType.value());
    }
    throw DifferentFieldTypeExpected("Cannot join {} and {}", toString(), otherDataType.toString());
}

std::string Float::toString() const
{
    return fmt::format("FLOAT{}", bits);
}

DataTypeRegistryReturnType DataTypeGeneratedRegistrar::RegisterFLOAT32DataType(DataTypeRegistryArguments)
{
    return std::make_unique<Float>(32);
}

DataTypeRegistryReturnType DataTypeGeneratedRegistrar::RegisterFLOAT64DataType(DataTypeRegistryArguments)
{
    return std::make_unique<Float>(64);
}

}
