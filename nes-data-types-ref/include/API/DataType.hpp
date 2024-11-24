//
// Created by ls on 11/24/24.
//

#pragma once
#include <concepts>
#include <cstddef>
#include <optional>
#include <ostream>
#include <string>
#include <utility>
#include <variant>

#include <Util/Logger/Formatter.hpp>
#include <sys/types.h>

namespace NES
{

struct FloatingType
{
    size_t precision;
    bool operator==(const FloatingType&) const = default;
    friend std::ostream& operator<<(std::ostream& os, const FloatingType& obj);
};

class Range
{
public:
    Range abs() const;
    void setMax(size_t newMax, bool negative);
    void setMax(ssize_t newMax);
    void setMin(size_t newMin, bool negative);
    void setMin(ssize_t newMin);

    [[nodiscard]] std::pair<size_t, bool> getMax() const;
    [[nodiscard]] std::pair<size_t, bool> getMin() const;

    bool operator==(const Range&) const = default;
    friend std::ostream& operator<<(std::ostream& os, const Range& obj);

private:
    size_t min = 0;
    bool negativeMin = false;
    size_t max = 0;
    bool negativeMax = false;
    void fixup();
};

struct IntegerType
{
    Range range;
    bool operator==(const IntegerType&) const = default;
    friend std::ostream& operator<<(std::ostream& os, const IntegerType& obj);
};

struct BooleanType
{
    bool operator==(const BooleanType&) const = default;
    friend std::ostream& operator<<(std::ostream& os, const BooleanType& obj);
};

struct CharacterType
{
    bool operator==(const CharacterType&) const = default;
    friend std::ostream& operator<<(std::ostream& os, const CharacterType& obj);
};

using BasicType = std::variant<FloatingType, IntegerType, BooleanType, CharacterType>;
bool operator==(const BasicType& lhs, const BasicType& rhs);
std::ostream& operator<<(std::ostream& os, const BasicType& obj);

template <typename T>
concept IsBasicType = std::constructible_from<BasicType, T>;

struct ScalarType
{
    explicit ScalarType(BasicType inner) : inner(std::move(inner)) { }
    BasicType inner;
    bool operator==(const ScalarType& rhs) const = default;
    friend std::ostream& operator<<(std::ostream& os, const ScalarType& obj);
};

struct VariableSizedType
{
    explicit VariableSizedType(BasicType inner) : inner(std::move(inner)) { }
    BasicType inner;
    bool operator==(const VariableSizedType&) const = default;
    friend std::ostream& operator<<(std::ostream& os, const VariableSizedType& obj);
};

struct FixedSizedType
{
    FixedSizedType(BasicType inner, size_t size) : inner(std::move(inner)), size(size) { }
    BasicType inner;
    size_t size;
    bool operator==(const FixedSizedType&) const = default;
    friend std::ostream& operator<<(std::ostream& os, const FixedSizedType& obj);
};

using AggregateType = std::variant<FixedSizedType, ScalarType, VariableSizedType>;
template <typename T>
std::optional<T> getUnderlying(const AggregateType& aggregateType)
{
    return std::visit(
        [](const auto& aggregate)
        {
            return std::visit(
                []<typename InnerType>(const InnerType& inner) -> std::optional<T>
                {
                    if constexpr (std::same_as<IntegerType, T>)
                    {
                        return inner;
                    }
                    else
                    {
                        return {};
                    }
                },
                aggregate);
        },
        aggregateType);
}

std::ostream& operator<<(std::ostream& os, const AggregateType& obj);
bool operator==(const AggregateType& lhs, const AggregateType& rhs);
bool operator==(const AggregateType& lhs, const IsBasicType auto& rhs)
{
    return lhs == ScalarType{rhs};
}

template <typename T>
concept IsAggregateType = std::constructible_from<AggregateType, T>;

struct DataType
{
    [[nodiscard]] std::optional<DataType> commonType(const DataType& other) const;
    bool operator==(const DataType&) const;
    bool operator==(const std::convertible_to<DataType> auto& t) const { return *this == static_cast<DataType>(t); }
    DataType(IsBasicType auto underlying) : underlying(ScalarType(std::move(underlying))) { }
    DataType(IsAggregateType auto underlying) : underlying(std::move(underlying)) { }

    template <IsBasicType T>
    std::optional<T> getUnderlying()
    {
        return getUnderlying<T>(underlying);
    }
    friend std::ostream& operator<<(std::ostream& os, const DataType& obj) { return os << obj.underlying; }
    AggregateType underlying;
};

/// Type Alias
BasicType uint64();
BasicType uint32();
BasicType uint16();
BasicType uint8();
BasicType int64();
BasicType int32();
BasicType int16();
BasicType int8();
BasicType character();
BasicType boolean();
BasicType float32();
BasicType float64();
AggregateType text();

/// Utils
bool isNumerical(const DataType& obj);
}

FMT_OSTREAM(NES::DataType);
FMT_OSTREAM(NES::AggregateType);
FMT_OSTREAM(NES::BasicType);
