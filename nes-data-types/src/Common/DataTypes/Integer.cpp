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
#include <memory>
#include <string>
#include <Util/Common.hpp>
#include <fmt/format.h>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/DataTypes/Float.hpp>
#include <Common/DataTypes/Integer.hpp>
#include <Common/DataTypes/Numeric.hpp>
#include <Common/DataTypes/Undefined.hpp>

#include <ErrorHandling.hpp>

namespace NES
{

bool Integer::operator==(const NES::DataType& other) const
{
    const auto otherInteger = dynamic_cast<const Integer*>(&other);
    if (otherInteger != nullptr)
    {
        return bits == otherInteger->bits && lowerBound == otherInteger->lowerBound && upperBound == otherInteger->upperBound;
    }
    return false;
}

DataTypePtr Integer::join(const DataTypePtr otherDataType)
{
    if (NES::Util::instanceOf<Undefined>(otherDataType))
    {
        return std::make_shared<Integer>(bits, lowerBound, upperBound);
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
    return fmt::format("{}{}", lowerBound == 0 ? "UINT" : "INT", std::to_string(bits));
}

}
