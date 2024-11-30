//
// Created by ls on 11/24/24.
//

#include <concepts>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <ostream>
#include <utility>
#include <variant>
#include <API/DataType.hpp>

namespace NES
{
template <std::unsigned_integral T>
BasicType standardIntegerTypes()
{
    Range range;
    range.setMin(std::numeric_limits<T>::min(), false);
    range.setMax(std::numeric_limits<T>::max(), false);
    return IntegerType{std::move(range)};
}
template <std::signed_integral T>
BasicType standardIntegerTypes()
{
    Range range;
    range.setMin(std::numeric_limits<T>::min(), true);
    range.setMax(std::numeric_limits<T>::max(), false);
    return IntegerType{std::move(range)};
}

Range Range::abs() const
{
    auto copy = *this;
    copy.negativeMin = false;
    copy.negativeMax = false;
    copy.min = 0;
    copy.max = std::max(min, max);
    return copy;
}

void Range::setMax(size_t newMax, bool negative)
{
    this->max = newMax;
    this->negativeMax = negative;

    fixup();
}

void Range::setMax(ssize_t newMax)
{
    setMax(std::abs(newMax), newMax < 0);
}

void Range::setMin(size_t newMin, bool negative)
{
    this->min = newMin;
    this->negativeMin = negative;
    fixup();
}

void Range::setMin(ssize_t newMin)
{
    setMin(std::abs(newMin), newMin < 0);
}
std::pair<size_t, bool> Range::getMax() const
{
    return std::make_pair(this->max, this->negativeMax);
}
std::pair<size_t, bool> Range::getMin() const
{
    return std::make_pair(this->min, this->negativeMin);
}
void Range::fixup()
{
    if (negativeMax && negativeMin)
    {
        auto tmpMin = min;
        min = std::max(min, max);
        max = std::min(tmpMin, max);
    }
    else if (!negativeMin && !negativeMax)
    {
        auto tmpMin = min;
        min = std::min(min, max);
        max = std::max(tmpMin, max);
    }
    else if (!negativeMin && negativeMax)
    {
        negativeMin = true;
        negativeMax = false;
        std::swap(min, max);
    }
}

bool operator==(const BasicType& lhs, const BasicType& rhs)
{
    return std::visit(
        []<typename T1, typename T2>(T1 t1, T2 t2)
        {
            if constexpr (std::is_same_v<T1, T2>)
            {
                return t1 == t2;
            }
            else
            {
                return false;
            }
        },
        lhs,
        rhs);
}
std::ostream& operator<<(std::ostream& os, const BasicType& obj)
{
    std::visit([&os](const auto& inner) { os << inner; }, obj);
    return os;
}

bool isNumerical(const DataType& obj)
{
    if (const auto* scalar = std::get_if<ScalarType>(&obj.underlying))
    {
        return std::holds_alternative<FloatingType>(scalar->inner) || std::holds_alternative<IntegerType>(scalar->inner);
    }
    return false;
}

std::ostream& operator<<(std::ostream& os, const AggregateType& obj)
{
    std::visit([&os](const auto& inner) { os << inner; }, obj);
    return os;
}
bool operator==(const AggregateType& lhs, const AggregateType& rhs)
{
    return std::visit(
        []<typename T1, typename T2>(T1 t1, T2 t2)
        {
            if constexpr (std::is_same_v<T1, T2>)
            {
                return t1 == t2;
            }
            else
            {
                return false;
            }
        },
        lhs,
        rhs);
}
bool DataType::operator==(const DataType& other) const
{
    return std::visit(
        []<typename T1, typename T2>(T1 t1, T2 t2)
        {
            if constexpr (std::is_same_v<T1, T2>)
            {
                return t1 == t2;
            }
            else
            {
                return false;
            }
        },
        this->underlying,
        other.underlying);
}
BasicType uint64()
{
    return BasicType{standardIntegerTypes<uint64_t>()};
}
BasicType uint32()
{
    return BasicType{standardIntegerTypes<uint32_t>()};
}
BasicType uint16()
{
    return BasicType{standardIntegerTypes<uint16_t>()};
}
BasicType uint8()
{
    return BasicType{standardIntegerTypes<uint8_t>()};
}
BasicType int64()
{
    return BasicType{standardIntegerTypes<int64_t>()};
}
BasicType int32()
{
    return BasicType{standardIntegerTypes<int32_t>()};
}
BasicType int16()
{
    return BasicType{standardIntegerTypes<int16_t>()};
}
BasicType int8()
{
    return BasicType{standardIntegerTypes<int8_t>()};
}
BasicType character()
{
    return BasicType{CharacterType{}};
}
BasicType boolean()
{
    return BasicType{BooleanType{}};
}
BasicType float32()
{
    return BasicType{FloatingType{32}};
}
BasicType float64()
{
    return BasicType{FloatingType{64}};
}
AggregateType text()
{
    return AggregateType{VariableSizedType{CharacterType{}}};
}
std::ostream& operator<<(std::ostream& os, const FloatingType&)
{
    return os << "FLOAT";
}
std::ostream& operator<<(std::ostream& os, const Range& obj)
{
    return os << "[" << (obj.negativeMin ? "-" : "") << obj.min << "," << (obj.negativeMax ? "-" : "") << obj.max << "]";
}
std::ostream& operator<<(std::ostream& os, const IntegerType&)
{
    return os << "INTEGER";
}
std::ostream& operator<<(std::ostream& os, const BooleanType&)
{
    return os << "BOOL";
}
std::ostream& operator<<(std::ostream& os, const CharacterType&)
{
    return os << "CHAR";
}
std::ostream& operator<<(std::ostream& os, const ScalarType& obj)
{
    return os << obj.inner;
}
std::ostream& operator<<(std::ostream& os, const VariableSizedType& obj)
{
    return os << obj.inner << "[]";
}
std::ostream& operator<<(std::ostream& os, const FixedSizedType& obj)
{
    return os << obj.inner << "[" << obj.size << "]";
}
}