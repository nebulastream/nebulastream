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

#include <optional>

#include <Util/Common.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/DataTypes/Float.hpp>
#include <Common/DataTypes/Integer.hpp>
#include <Common/DataTypes/Numeric.hpp>


namespace NES
{

Numeric::Numeric(int8_t bits) : bits(bits)
{
}

int8_t Numeric::getBits() const
{
    return bits;
}

std::optional<DataTypePtr> Numeric::inferDataType(const Numeric& left, const Numeric& right)
{
    /// We infer the data types between two numerics by following the c++ rules. For example, anything below i32 will be casted to a i32.
    /// Unsigned and signed of the same data type will be casted to the unsigned data type.
    /// For a playground, please take a look at the godbolt link: https://godbolt.org/z/j1cTfczbh

    constexpr int8_t sizeOfIntInBits = sizeof(int32_t) * 8;
    constexpr int8_t sizeOfLongInBits = sizeof(int64_t) * 8;


    /// If left is a float, the result is a float or double depending on the bits of the left float
    if (Util::instanceOf<const Float>(left) && Util::instanceOf<const Integer>(right))
    {
        return (left.bits == sizeOfIntInBits ? DataTypeFactory::createFloat() : DataTypeFactory::createDouble());
    }

    if (Util::instanceOf<const Integer>(left) && Util::instanceOf<const Float>(right))
    {
        return (right.bits == sizeOfIntInBits ? DataTypeFactory::createFloat() : DataTypeFactory::createDouble());
    }


    if (Util::instanceOf<const Float>(right) && Util::instanceOf<const Float>(left))
    {
        return (
            left.bits == sizeOfLongInBits || right.bits == sizeOfLongInBits ? DataTypeFactory::createDouble()
                                                                            : DataTypeFactory::createFloat());
    }

    if (Util::instanceOf<const Integer>(right) && Util::instanceOf<const Integer>(left))
    {
        /// We need to still cast here to an integer, as the lowerBound is a member of Integer and not of Numeric
        const auto leftInt = Util::as<const Integer>(left);
        const auto rightInt = Util::as<const Integer>(right);
        const auto leftSign = leftInt.lowerBound < 0;
        const auto rightSign = rightInt.lowerBound < 0;
        const auto leftBits = leftInt.getBits();
        const auto rightBits = rightInt.getBits();
        if (leftBits < sizeOfIntInBits && rightBits < sizeOfIntInBits)
        {
            return DataTypeFactory::createInt32();
        }

        if (leftBits == sizeOfIntInBits && rightBits < sizeOfIntInBits)
        {
            return (leftSign ? DataTypeFactory::createInt32() : DataTypeFactory::createUInt32());
        }

        if (leftBits < sizeOfIntInBits && rightBits == sizeOfIntInBits)
        {
            return (rightSign ? DataTypeFactory::createInt32() : DataTypeFactory::createUInt32());
        }

        if (leftBits == sizeOfIntInBits && rightBits == sizeOfIntInBits)
        {
            return ((leftSign && rightSign) ? DataTypeFactory::createInt32() : DataTypeFactory::createUInt32());
        }

        if (leftBits == sizeOfLongInBits && rightBits < sizeOfLongInBits)
        {
            return (leftSign ? DataTypeFactory::createInt64() : DataTypeFactory::createUInt64());
        }

        if (leftBits < sizeOfLongInBits && rightBits == sizeOfLongInBits)
        {
            return (rightSign ? DataTypeFactory::createInt64() : DataTypeFactory::createUInt64());
        }

        if (leftBits == sizeOfLongInBits && rightBits == sizeOfLongInBits)
        {
            return ((leftSign && rightSign) ? DataTypeFactory::createInt64() : DataTypeFactory::createUInt64());
        }
    }

    return {};
}

}
