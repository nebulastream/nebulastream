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
#include <Nautilus/DataTypes/VarVal.hpp>

#include <concepts>
#include <cstdint>
#include <ostream>
#include <string>
#include <type_traits>
#include <utility>
#include <variant>
#include <DataTypes/DataType.hpp>
#include <Nautilus/DataTypes/DataTypesUtil.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
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

VarVal::VarVal(const VarVal& other) : value(other.value), null(other.null), nullable(other.nullable)
{
}

VarVal::VarVal(VarVal&& other) noexcept : value(std::move(other.value)), null(std::move(other.null)), nullable(std::move(other.nullable))
{
}

VarVal& VarVal::operator=(const VarVal& other)
{
    if (value.index() != other.value.index())
    {
        throw UnknownOperation("Not allowed to change the data type via the assignment operator, please use castToType()!");
    }
    value = other.value;
    null = other.null;
    nullable = other.nullable;
    return *this;
}

VarVal& VarVal::operator=(VarVal&& other) /// NOLINT, as we need to have the option of throwing
{
    if (value.index() != other.value.index())
    {
        throw UnknownOperation("Not allowed to change the data type via the assignment operator, please use castToType()!");
    }
    value = std::move(other.value);
    null = std::move(other.null);
    nullable = std::move(other.nullable);
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
    return null;
}

bool VarVal::isNullable() const
{
    return nullable;
}

VarVal::operator bool() const
{
    const nautilus::val<int> valueInt = std::visit(
        []<typename T>(T& val) -> nautilus::val<int>
        {
            if constexpr (requires(T t) { t == nautilus::val<bool>(true); })
            {
                /// We have to do it like this. The reason is that during the comparison of the two values, @val is NOT converted to a bool
                /// but rather the val<bool>(false) is converted to std::common_type<T, bool>. This is a problem for any val that is not set to 1.
                /// As we will then compare val == 1, which will always be false.
                return nautilus::val<int>{!(val == nautilus::val<bool>(false))};
            }
            else
            {
                throw UnknownOperation();
            }
        },
        value);
    const auto retValue
        = nautilus::select((not(isNullable() and isNull()) and valueInt) == 1, nautilus::val<bool>{true}, nautilus::val<bool>{false});
    return retValue;
}

VarVal VarVal::castToType(const DataType::Type type) const
{
    switch (type)
    {
        case DataType::Type::CHAR: {
            return {cast<nautilus::val<char>>(), nullable, null};
        }
        case DataType::Type::BOOLEAN: {
            return {cast<nautilus::val<bool>>(), nullable, null};
        }
        case DataType::Type::INT8: {
            return {cast<nautilus::val<int8_t>>(), nullable, null};
        }
        case DataType::Type::INT16: {
            return {cast<nautilus::val<int16_t>>(), nullable, null};
        }
        case DataType::Type::INT32: {
            return {cast<nautilus::val<int32_t>>(), nullable, null};
        }
        case DataType::Type::INT64: {
            return {cast<nautilus::val<int64_t>>(), nullable, null};
        }
        case DataType::Type::UINT8: {
            return {cast<nautilus::val<uint8_t>>(), nullable, null};
        }
        case DataType::Type::UINT16: {
            return {cast<nautilus::val<uint16_t>>(), nullable, null};
        }
        case DataType::Type::UINT32: {
            return {cast<nautilus::val<uint32_t>>(), nullable, null};
        }
        case DataType::Type::UINT64: {
            return {cast<nautilus::val<uint64_t>>(), nullable, null};
        }
        case DataType::Type::FLOAT32: {
            return {cast<nautilus::val<float>>(), nullable, null};
        }
        case DataType::Type::FLOAT64: {
            return {cast<nautilus::val<double>>(), nullable, null};
        }
        case DataType::Type::VARSIZED: {
            return {cast<VariableSizedData>(), nullable, null};
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
    return VarVal::readVarValFromMemory(memRef, type, nautilus::val<bool>(false));
}

VarVal VarVal::readVarValFromMemory(const nautilus::val<int8_t*>& memRef, const DataType type, const nautilus::val<bool>& null)
{
    switch (type.type)
    {
        case DataType::Type::BOOLEAN: {
            return {readValueFromMemRef<bool>(memRef), type.nullable, null};
        }
        case DataType::Type::INT8: {
            return {readValueFromMemRef<int8_t>(memRef), type.nullable, null};
        }
        case DataType::Type::INT16: {
            return {readValueFromMemRef<int16_t>(memRef), type.nullable, null};
        }
        case DataType::Type::INT32: {
            return {readValueFromMemRef<int32_t>(memRef), type.nullable, null};
        }
        case DataType::Type::INT64: {
            return {readValueFromMemRef<int64_t>(memRef), type.nullable, null};
        }
        case DataType::Type::CHAR: {
            return {readValueFromMemRef<char>(memRef), type.nullable, null};
        }
        case DataType::Type::UINT8: {
            return {readValueFromMemRef<uint8_t>(memRef), type.nullable, null};
        }
        case DataType::Type::UINT16: {
            return {readValueFromMemRef<uint16_t>(memRef), type.nullable, null};
        }
        case DataType::Type::UINT32: {
            return {readValueFromMemRef<uint32_t>(memRef), type.nullable, null};
        }
        case DataType::Type::UINT64: {
            return {readValueFromMemRef<uint64_t>(memRef), type.nullable, null};
        }
        case DataType::Type::FLOAT32: {
            return {readValueFromMemRef<float>(memRef), type.nullable, null};
        }
        case DataType::Type::FLOAT64: {
            return {readValueFromMemRef<double>(memRef), type.nullable, null};
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
                return VarVal{
                    nautilus::select(condition, trueUnderlying, falseUnderlying),
                    trueValue.nullable or falseValue.nullable,
                    nautilus::select(condition, trueValue.null, falseValue.null)};
            }

            if constexpr (std::same_as<LHS, RHS> && std::same_as<LHS, VariableSizedData>)
            {
                return VarVal{
                    VariableSizedData{
                        nautilus::select(condition, trueUnderlying.getContent(), falseUnderlying.getContent()),
                        nautilus::select(condition, trueUnderlying.getSize(), falseUnderlying.getSize())},
                    trueValue.nullable or falseValue.nullable,
                    nautilus::select(condition, trueValue.null, falseValue.null)};
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
    if (varVal.null)
    {
        return os << "NULL";
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
            [leftIsNullable = this->isNullable(), \
             rightIsNullable = other.isNullable(), \
             leftIsNull = this->isNull(), \
             rightIsNull = other.isNull()]<typename LHS, typename RHS>(const LHS& lhsVal, const RHS& rhsVal) \
            { \
                if constexpr (requires(LHS lhs, RHS rhs) { lhs op rhs; }) \
                { \
                    if (leftIsNullable or rightIsNullable) \
                    { \
                        using ResultType = decltype(lhsVal op rhsVal); \
                        const auto newValue = nautilus::select(leftIsNull or rightIsNull, ResultType{0}, lhsVal op rhsVal); \
                        return VarVal{newValue, true, leftIsNull or rightIsNull}; \
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
            [isNullable = isNullable(), isNull = isNull()]<typename RHS>(const RHS& rhsVal) \
            { \
                if constexpr (!requires(RHS rhs) { op rhs; }) \
                { \
                    throw UnknownOperation("VarVal operation not implemented: " #op "{}", NAMEOF_TYPE(RHS)); \
                    return VarVal{detail::var_val_t(rhsVal), true, false}; \
                } \
                else \
                { \
                    if (isNullable) \
                    { \
                        using ResultType = decltype(op rhsVal); \
                        const auto newValue = nautilus::select(isNull, ResultType{0}, op rhsVal); \
                        return VarVal{newValue, true, isNull}; \
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
        [leftIsNullable = this->isNullable(),
         rightIsNullable = other.isNullable(),
         leftIsNull = this->isNull(),
         rightIsNull = other.isNull()]<typename LHS, typename RHS>(const LHS& lhsVal, const RHS& rhsVal)
        {
            if constexpr (requires(LHS l, RHS r) { l / r; })
            {
                if (leftIsNullable or rightIsNullable)
                {
                    if (rhsVal == RHS{0} and not rightIsNull)
                    {
                        nautilus::invoke(+[] { throw ArithmeticalError("Can not divide by zero!"); });
                    }

                    /// Using safe denominator if it is zero and rhs is null
                    const auto safeDenominator = nautilus::select(rhsVal == RHS{0}, RHS{1}, rhsVal);
                    using ResultType = decltype(lhsVal / rhsVal);
                    const auto newValue = nautilus::select(leftIsNull or rightIsNull, ResultType{0}, lhsVal / safeDenominator);
                    return VarVal{newValue, true, leftIsNull or rightIsNull};
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
        [leftIsNullable = this->isNullable(),
         rightIsNullable = other.isNullable(),
         leftIsNull = this->isNull(),
         rightIsNull = other.isNull()]<typename LHS, typename RHS>(const LHS& lhsVal, const RHS& rhsVal)
        {
            if constexpr (requires(LHS l, RHS r) { l % r; })
            {
                if (leftIsNullable or rightIsNullable)
                {
                    if (rhsVal == RHS{0} and not rightIsNull)
                    {
                        nautilus::invoke(+[] { throw ArithmeticalError("Can not modulo by zero!"); });
                    }

                    /// Using safe denominator if it is zero and rhs is null
                    const auto safeDenominator = nautilus::select(rhsVal == RHS{0}, RHS{1}, rhsVal);
                    using ResultType = decltype(lhsVal % rhsVal);
                    const auto newValue = nautilus::select(leftIsNull or rightIsNull, ResultType{0}, lhsVal % safeDenominator);
                    return VarVal{newValue, true, leftIsNull or rightIsNull};
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
