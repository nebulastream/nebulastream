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
#include <DataTypes/VarVal.hpp>

#include <concepts>
#include <cstdint>
#include <optional>
#include <ostream>
#include <string>
#include <type_traits>
#include <utility>
#include <variant>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypesUtil.hpp>
#include <DataTypes/VariableSizedData.hpp>
#include <magic_enum/magic_enum.hpp>
#include <nautilus/select.hpp>
#include <nautilus/std/ostream.h>
#include <nautilus/val.hpp>
#include <nautilus/val_ptr.hpp>
#include <ErrorHandling.hpp>
#include <nameof.hpp>
#include <val_arith.hpp>
#include <val_bool.hpp>
#include <val_concepts.hpp>

namespace NES
{

VarVal::VarVal(VarVal&& other) noexcept : value(std::move(other.value)), nullFlag(std::move(other.nullFlag))
{
}

namespace
{
/// The null flag of the result of an operation on two values: traced only if one of them is nullable.
std::optional<nautilus::val<bool>>
combineNullFlags(const std::optional<nautilus::val<bool>>& lhs, const std::optional<nautilus::val<bool>>& rhs)
{
    if (lhs.has_value() and rhs.has_value())
    {
        return lhs.value() or rhs.value();
    }
    if (lhs.has_value())
    {
        return lhs;
    }
    return rhs;
}

/// Assigns the null flag of the assigned VarVal. An engaged flag stays engaged, so the traced variable is kept on every path.
void assignNullFlag(std::optional<nautilus::val<bool>>& target, const std::optional<nautilus::val<bool>>& source)
{
    if (target.has_value())
    {
        if (source.has_value())
        {
            *target = *source;
        }
        else
        {
            *target = nautilus::val<bool>{false};
        }
    }
    else if (source.has_value())
    {
        target.emplace(*source);
    }
}
}

VarVal& VarVal::operator=(const VarVal& other)
{
    if (value.index() != other.value.index())
    {
        throw UnknownOperation("Not allowed to change the data type via the assignment operator, please use castToType()!");
    }
    value = other.value;
    assignNullFlag(nullFlag, other.nullFlag);
    return *this;
}

VarVal& VarVal::operator=(VarVal&& other) /// NOLINT, as we need to have the option of throwing
{
    if (value.index() != other.value.index())
    {
        throw UnknownOperation("Not allowed to change the data type via the assignment operator, please use castToType()!");
    }
    value = std::move(other.value);
    assignNullFlag(nullFlag, other.nullFlag);
    return *this;
}

void VarVal::writeToMemory(const nautilus::val<int8_t*>& memRef) const
{
    std::visit(
        [&]<typename ValType>(const ValType& val)
        {
            if constexpr (std::is_same_v<ValType, VariableSizedData>)
            {
                throw UnknownOperation(std::string("VarVal T::operation=(val) not implemented for VariableSizedData"));
            }
            else
            {
                *static_cast<nautilus::val<typename ValType::raw_type*>>(memRef) = val;
            }
        },
        value);
}

nautilus::val<bool> VarVal::isNull() const
{
    if (nullFlag.has_value())
    {
        return *nullFlag;
    }
    return nautilus::val<bool>{false};
}

bool VarVal::isNullable() const
{
    return nullFlag.has_value();
}

VarVal::operator bool() const
{
    const nautilus::val<bool> isTrue = std::visit(
        []<typename T>(const T& val) -> nautilus::val<bool>
        {
            if constexpr (std::same_as<T, nautilus::val<bool>>)
            {
                return val;
            }
            else if constexpr (requires(T candidate) { candidate == nautilus::val<bool>(true); })
            {
                /// We have to do it like this. The reason is that during the comparison of the two values, @val is NOT converted to a bool
                /// but rather the val<bool>(false) is converted to std::common_type<T, bool>. This is a problem for any val that is not set to 1.
                /// As we will then compare val == 1, which will always be false.
                return !(val == nautilus::val<bool>(false));
            }
            else
            {
                throw UnknownOperation();
            }
        },
        value);
    if (nullFlag.has_value())
    {
        return static_cast<bool>(isTrue and not*nullFlag);
    }
    return static_cast<bool>(isTrue);
}

VarVal VarVal::castToType(const DataType::Type type) const
{
    switch (type)
    {
        case DataType::Type::CHAR: {
            return {detail::var_val_t{getRawValueAs<nautilus::val<char>>()}, nullFlag};
        }
        case DataType::Type::BOOLEAN: {
            return {detail::var_val_t{getRawValueAs<nautilus::val<bool>>()}, nullFlag};
        }
        case DataType::Type::INT8: {
            return {detail::var_val_t{getRawValueAs<nautilus::val<int8_t>>()}, nullFlag};
        }
        case DataType::Type::INT16: {
            return {detail::var_val_t{getRawValueAs<nautilus::val<int16_t>>()}, nullFlag};
        }
        case DataType::Type::INT32: {
            return {detail::var_val_t{getRawValueAs<nautilus::val<int32_t>>()}, nullFlag};
        }
        case DataType::Type::INT64: {
            return {detail::var_val_t{getRawValueAs<nautilus::val<int64_t>>()}, nullFlag};
        }
        case DataType::Type::UINT8: {
            return {detail::var_val_t{getRawValueAs<nautilus::val<uint8_t>>()}, nullFlag};
        }
        case DataType::Type::UINT16: {
            return {detail::var_val_t{getRawValueAs<nautilus::val<uint16_t>>()}, nullFlag};
        }
        case DataType::Type::UINT32: {
            return {detail::var_val_t{getRawValueAs<nautilus::val<uint32_t>>()}, nullFlag};
        }
        case DataType::Type::UINT64: {
            return {detail::var_val_t{getRawValueAs<nautilus::val<uint64_t>>()}, nullFlag};
        }
        case DataType::Type::FLOAT32: {
            return {detail::var_val_t{getRawValueAs<nautilus::val<float>>()}, nullFlag};
        }
        case DataType::Type::FLOAT64: {
            return {detail::var_val_t{getRawValueAs<nautilus::val<double>>()}, nullFlag};
        }
        case DataType::Type::VARSIZED: {
            return {detail::var_val_t{getRawValueAs<VariableSizedData>()}, nullFlag};
        }
        case DataType::Type::UNDEFINED:
            throw UnknownDataType("Not supporting reading {} data type from memory.", magic_enum::enum_name(type));
    }
    std::unreachable();
}

VarVal VarVal::readNonNullableVarValFromMemory(const nautilus::val<int8_t*>& memRef, const DataType type)
{
    PRECONDITION(
        not type.nullable,
        "This function can only be called if the data type is not nullable. Please use the overloaded function readVarValFromMemory(const "
        "nautilus::val<int8_t*>&, DataType, const nautilus::val<bool>&) instead!");
    return readVarValFromMemoryImpl(memRef, type, false);
}

VarVal VarVal::readVarValFromMemory(const nautilus::val<int8_t*>& memRef, const DataType type, nautilus::val<bool> null)
{
    return readVarValFromMemoryImpl(memRef, type, std::move(null));
}

template <NullArgument Null>
VarVal VarVal::readVarValFromMemoryImpl(const nautilus::val<int8_t*>& memRef, const DataType type, Null&& null)
{
    switch (type.type)
    {
        case DataType::Type::BOOLEAN: {
            return {readValueFromMemRef<bool>(memRef), type.nullable, std::forward<Null>(null)};
        }
        case DataType::Type::INT8: {
            return {readValueFromMemRef<int8_t>(memRef), type.nullable, std::forward<Null>(null)};
        }
        case DataType::Type::INT16: {
            return {readValueFromMemRef<int16_t>(memRef), type.nullable, std::forward<Null>(null)};
        }
        case DataType::Type::INT32: {
            return {readValueFromMemRef<int32_t>(memRef), type.nullable, std::forward<Null>(null)};
        }
        case DataType::Type::INT64: {
            return {readValueFromMemRef<int64_t>(memRef), type.nullable, std::forward<Null>(null)};
        }
        case DataType::Type::CHAR: {
            return {readValueFromMemRef<char>(memRef), type.nullable, std::forward<Null>(null)};
        }
        case DataType::Type::UINT8: {
            return {readValueFromMemRef<uint8_t>(memRef), type.nullable, std::forward<Null>(null)};
        }
        case DataType::Type::UINT16: {
            return {readValueFromMemRef<uint16_t>(memRef), type.nullable, std::forward<Null>(null)};
        }
        case DataType::Type::UINT32: {
            return {readValueFromMemRef<uint32_t>(memRef), type.nullable, std::forward<Null>(null)};
        }
        case DataType::Type::UINT64: {
            return {readValueFromMemRef<uint64_t>(memRef), type.nullable, std::forward<Null>(null)};
        }
        case DataType::Type::FLOAT32: {
            return {readValueFromMemRef<float>(memRef), type.nullable, std::forward<Null>(null)};
        }
        case DataType::Type::FLOAT64: {
            return {readValueFromMemRef<double>(memRef), type.nullable, std::forward<Null>(null)};
        }
        case DataType::Type::VARSIZED:
        case DataType::Type::UNDEFINED:
            throw UnknownDataType("Not supporting reading {} data type from memory.", magic_enum::enum_name(type.type));
    }
    std::unreachable();
}

VarVal VarVal::select(const nautilus::val<bool>& condition, const VarVal& trueValue, const VarVal& falseValue)
{
    return std::visit(
        [&]<typename LHS, typename RHS>(const LHS& trueUnderlying, const RHS& falseUnderlying) -> VarVal
        {
            if constexpr (std::same_as<LHS, RHS> && !std::same_as<LHS, VariableSizedData>)
            {
                const bool nullable = trueValue.isNullable() or falseValue.isNullable();
                return VarVal{
                    detail::var_val_t{nautilus::select(condition, trueUnderlying, falseUnderlying)},
                    nullable ? std::optional{nautilus::select(condition, trueValue.isNull(), falseValue.isNull())} : std::nullopt};
            }

            if constexpr (std::same_as<LHS, RHS> && std::same_as<LHS, VariableSizedData>)
            {
                const bool nullable = trueValue.isNullable() or falseValue.isNullable();
                return VarVal{
                    detail::var_val_t{VariableSizedData{
                        nautilus::select(condition, trueUnderlying.getContent(), falseUnderlying.getContent()),
                        nautilus::select(condition, trueUnderlying.getSize(), falseUnderlying.getSize())}},
                    nullable ? std::optional{nautilus::select(condition, trueValue.isNull(), falseValue.isNull())} : std::nullopt};
            }
            throw UnknownOperation("select with different types! True: {} vs. False: {}", NAMEOF_TYPE(LHS), NAMEOF_TYPE(RHS));
            std::unreachable();
        }

        ,
        trueValue.value,
        falseValue.value);
}

nautilus::val<std::ostream>& operator<<(nautilus::val<std::ostream>& os, const VarVal& varVal)
{
    /// Nested: nautilus' `and` on a val<bool> does not short-circuit, so it would read the flag of a non-nullable value.
    if (varVal.nullFlag.has_value())
    {
        if (*varVal.nullFlag)
        {
            return os << "NULL";
        }
    }

    return std::visit(
        [&os]<typename T>(T& value) -> nautilus::val<std::ostream>&
        {
            /// If the T is of type uint8_t or int8_t, we want to convert it to an integer to print it as an integer and not as a char
            using Tremoved = std::remove_cvref_t<T>;
            if constexpr (
                std::is_same_v<Tremoved, nautilus::val<uint8_t>> || std::is_same_v<Tremoved, nautilus::val<int8_t>>
                || std::is_same_v<Tremoved, nautilus::val<unsigned char>> || std::is_same_v<Tremoved, nautilus::val<char>>)
            {
                return os.operator<<(static_cast<nautilus::val<int>>(value));
            }
            else if constexpr (requires(typename T::basic_type type) { os << (nautilus::val<typename T::basic_type>(type)); })
            {
                return os.operator<<(nautilus::val<typename T::basic_type>(value));
            }
            else if constexpr (requires { operator<<(os, value); })
            {
                return operator<<(os, value);
            }
            else
            {
                throw UnknownOperation();
                std::unreachable();
            }
        },
        varVal.value);
}

#define DEFINE_OPERATOR_VAR_VAL_BINARY(operatorName, op) \
    VarVal VarVal::operatorName(const VarVal& other) const \
    { \
        return std::visit( \
            [this, &other]<typename LHS, typename RHS>(const LHS& lhsVal, const RHS& rhsVal) \
            { \
                if constexpr (requires(LHS lhs, RHS rhs) { lhs op rhs; }) \
                { \
                    if (auto resultNull = combineNullFlags(nullFlag, other.nullFlag)) \
                    { \
                        using ResultType = decltype(lhsVal op rhsVal); \
                        auto newValue = nautilus::select(*resultNull, ResultType{0}, lhsVal op rhsVal); \
                        return VarVal{std::move(newValue), true, std::move(*resultNull)}; \
                    } \
                    return VarVal{lhsVal op rhsVal, false, false}; \
                } \
                else \
                { \
                    throw UnknownOperation("VarVal operation not implemented: {} " #op " {}", NAMEOF_TYPE(LHS), NAMEOF_TYPE(RHS)); \
                    return VarVal{lhsVal, true, true}; \
                } \
            }, \
            this->value, \
            other.value); \
    }
#define DEFINE_OPERATOR_VAR_VAL_UNARY(operatorName, op) \
    VarVal VarVal::operatorName() const \
    { \
        return std::visit( \
            [this]<typename RHS>(const RHS& rhsVal) \
            { \
                if constexpr (!requires(RHS rhs) { op rhs; }) \
                { \
                    throw UnknownOperation("VarVal operation not implemented: " #op "{}", NAMEOF_TYPE(RHS)); \
                    return VarVal{detail::var_val_t(rhsVal), std::optional{nautilus::val<bool>{false}}}; \
                } \
                else \
                { \
                    if (nullFlag.has_value()) \
                    { \
                        using ResultType = decltype(op rhsVal); \
                        auto newValue = nautilus::select(*nullFlag, ResultType{0}, op rhsVal); \
                        return VarVal{std::move(newValue), true, *nullFlag}; \
                    } \
                    return VarVal{op rhsVal, false, false}; \
                } \
            }, \
            this->value); \
    }

/// With div, we might have a problem if we divide by 0 even though the VarVal is null. To handle this special case,
/// we create a custom method for this and do not use the #define for the other binary operators
VarVal VarVal::operator/(const VarVal& other) const
{
    return std::visit(
        [this, &other]<typename LHS, typename RHS>(const LHS& lhsVal, const RHS& rhsVal)
        {
            if constexpr (requires(LHS l, RHS r) { l / r; })
            {
                if (auto resultNull = combineNullFlags(nullFlag, other.nullFlag))
                {
                    if (rhsVal == RHS{0} and not other.isNull())
                    {
                        nautilus::invoke(+[] { throw ArithmeticalError("Can not divide by zero!"); });
                    }

                    /// Using safe denominator if it is zero and rhs is null
                    const auto safeDenominator = nautilus::select(rhsVal == RHS{0}, RHS{1}, rhsVal);
                    using ResultType = decltype(lhsVal / rhsVal);
                    auto newValue = nautilus::select(*resultNull, ResultType{0}, lhsVal / safeDenominator);
                    return VarVal{std::move(newValue), true, std::move(*resultNull)};
                }
                return VarVal{lhsVal / rhsVal, false, false};
            }
            else
            {
                throw UnknownOperation(
                    std::string("VarVal operation not implemented: ") + " " + +" " + typeid(LHS).name() + " " + typeid(RHS).name());
                return VarVal{lhsVal, true, true};
            }
        },
        this->value,
        other.value);
}

/// With mod, we might have a problem if we modulo by 0 even though the VarVal is null. To handle this special case,
/// we create a custom method for this and do not use the #define for the other binary operators
VarVal VarVal::operator%(const VarVal& other) const
{
    return std::visit(
        [this, &other]<typename LHS, typename RHS>(const LHS& lhsVal, const RHS& rhsVal)
        {
            if constexpr (requires(LHS l, RHS r) { l % r; })
            {
                if (auto resultNull = combineNullFlags(nullFlag, other.nullFlag))
                {
                    if (rhsVal == RHS{0} and not other.isNull())
                    {
                        nautilus::invoke(+[] { throw ArithmeticalError("Can not modulo by zero!"); });
                    }

                    /// Using safe denominator if it is zero and rhs is null
                    const auto safeDenominator = nautilus::select(rhsVal == RHS{0}, RHS{1}, rhsVal);
                    using ResultType = decltype(lhsVal % rhsVal);
                    auto newValue = nautilus::select(*resultNull, ResultType{0}, lhsVal % safeDenominator);
                    return VarVal{std::move(newValue), true, std::move(*resultNull)};
                }
                return VarVal{lhsVal % rhsVal, false, false};
            }
            else
            {
                throw UnknownOperation(
                    std::string("VarVal operation not implemented: ") + " " + +" " + typeid(LHS).name() + " " + typeid(RHS).name());
                return VarVal{lhsVal, true, true};
            }
        },
        this->value,
        other.value);
}

/// Defining operations on VarVal. In the macro, we use std::variant and std::visit to automatically call the already
/// existing operations on the underlying nautilus::val<> data types.
/// For the VarSizedDataType, we define custom operations in the class itself.
DEFINE_OPERATOR_VAR_VAL_BINARY(operator+, +);
DEFINE_OPERATOR_VAR_VAL_BINARY(operator-, -);
DEFINE_OPERATOR_VAR_VAL_BINARY(operator*, *);
DEFINE_OPERATOR_VAR_VAL_BINARY(operator==, ==);
DEFINE_OPERATOR_VAR_VAL_BINARY(operator!=, !=);
DEFINE_OPERATOR_VAR_VAL_BINARY(operator&&, &&);
DEFINE_OPERATOR_VAR_VAL_BINARY(operator||, ||);
DEFINE_OPERATOR_VAR_VAL_BINARY(operator<, <);
DEFINE_OPERATOR_VAR_VAL_BINARY(operator>, >);
DEFINE_OPERATOR_VAR_VAL_BINARY(operator<=, <=);
DEFINE_OPERATOR_VAR_VAL_BINARY(operator>=, >=);
DEFINE_OPERATOR_VAR_VAL_BINARY(operator&, &);
DEFINE_OPERATOR_VAR_VAL_BINARY(operator|, |);
DEFINE_OPERATOR_VAR_VAL_BINARY(operator^, ^);
DEFINE_OPERATOR_VAR_VAL_BINARY(operator<<, <<);
DEFINE_OPERATOR_VAR_VAL_BINARY(operator>>, >>);
DEFINE_OPERATOR_VAR_VAL_UNARY(operator!, !);
}
