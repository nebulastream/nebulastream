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

#include <cstdint>
#include <memory>
#include <optional>
#include <Util/Common.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeProvider.hpp>
#include <Common/DataTypes/Float.hpp>
#include <Common/DataTypes/Integer.hpp>
#include <Common/DataTypes/Numeric.hpp>

namespace NES
{

Numeric::Numeric(const int8_t bits) : bits(bits)
{
}

int8_t Numeric::getBits() const
{
    return bits;
}

std::optional<std::shared_ptr<DataType>> Numeric::inferDataType(const Numeric& left, const Numeric& right)
{
    /// We infer the data types between two numerics by following the c++ rules. For example, anything below i32 will be casted to a i32.
    /// Unsigned and signed of the same data type will be casted to the unsigned data type.
    /// For a playground, please take a look at the godbolt link: https://godbolt.org/z/j1cTfczbh

    constexpr int8_t sizeOfIntInBits = sizeof(int32_t) * 8;
    constexpr int8_t sizeOfLongInBits = sizeof(int64_t) * 8;


    /// If left is a float, the result is a float or double depending on the bits of the left float
    if (Util::instanceOf<const Float>(left) && Util::instanceOf<const Integer>(right))
    {
        return (
            left.bits == sizeOfIntInBits ? DataTypeProvider::provideDataType(LogicalType::FLOAT32)
                                         : DataTypeProvider::provideDataType(LogicalType::FLOAT64));
    }

    if (Util::instanceOf<const Integer>(left) && Util::instanceOf<const Float>(right))
    {
        return (
            right.bits == sizeOfIntInBits ? DataTypeProvider::provideDataType(LogicalType::FLOAT32)
                                          : DataTypeProvider::provideDataType(LogicalType::FLOAT64));
    }


    if (Util::instanceOf<const Float>(right) && Util::instanceOf<const Float>(left))
    {
        return (
            left.bits == sizeOfLongInBits || right.bits == sizeOfLongInBits ? DataTypeProvider::provideDataType(LogicalType::FLOAT64)
                                                                            : DataTypeProvider::provideDataType(LogicalType::FLOAT32));
    }

    if (Util::instanceOf<const Integer>(right) && Util::instanceOf<const Integer>(left))
    {
        /// We need to still cast here to an integer, as the lowerBound is a member of Integer and not of Numeric
        const auto leftInt = Util::as<const Integer>(left);
        const auto rightInt = Util::as<const Integer>(right);
        const auto leftSign = leftInt.getIsSigned();
        const auto rightSign = rightInt.getIsSigned();
        const auto leftBits = leftInt.getBits();
        const auto rightBits = rightInt.getBits();
        if (leftBits < sizeOfIntInBits && rightBits < sizeOfIntInBits)
        {
            return DataTypeProvider::provideDataType(LogicalType::INT32);
        }

        if (leftBits == sizeOfIntInBits && rightBits < sizeOfIntInBits)
        {
            return (
                leftSign ? DataTypeProvider::provideDataType(LogicalType::INT32) : DataTypeProvider::provideDataType(LogicalType::UINT32));
        }

        if (leftBits < sizeOfIntInBits && rightBits == sizeOfIntInBits)
        {
            return (
                rightSign ? DataTypeProvider::provideDataType(LogicalType::INT32) : DataTypeProvider::provideDataType(LogicalType::UINT32));
        }

        if (leftBits == sizeOfIntInBits && rightBits == sizeOfIntInBits)
        {
            return (
                (leftSign && rightSign) ? DataTypeProvider::provideDataType(LogicalType::INT32)
                                        : DataTypeProvider::provideDataType(LogicalType::UINT32));
        }

        if (leftBits == sizeOfLongInBits && rightBits < sizeOfLongInBits)
        {
            return (
                leftSign ? DataTypeProvider::provideDataType(LogicalType::INT64) : DataTypeProvider::provideDataType(LogicalType::UINT64));
        }

        if (leftBits < sizeOfLongInBits && rightBits == sizeOfLongInBits)
        {
            return (
                rightSign ? DataTypeProvider::provideDataType(LogicalType::INT64) : DataTypeProvider::provideDataType(LogicalType::UINT64));
        }

        if (leftBits == sizeOfLongInBits && rightBits == sizeOfLongInBits)
        {
            return (
                (leftSign && rightSign) ? DataTypeProvider::provideDataType(LogicalType::INT64)
                                        : DataTypeProvider::provideDataType(LogicalType::UINT64));
        }
    }

    return {};
}

}
