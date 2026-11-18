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
#include <limits>
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

VarVal::VarVal(VarVal&& other) noexcept : value(std::move(other.value)), null(std::move(other.null)), nullable(other.nullable)
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
    null = other.null;
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
            return {getRawValueAs<nautilus::val<char>>(), nullable, null};
        }
        case DataType::Type::BOOLEAN: {
            return {getRawValueAs<nautilus::val<bool>>(), nullable, null};
        }
        case DataType::Type::INT8: {
            return {getRawValueAs<nautilus::val<int8_t>>(), nullable, null};
        }
        case DataType::Type::INT16: {
            return {getRawValueAs<nautilus::val<int16_t>>(), nullable, null};
        }
        case DataType::Type::INT32: {
            return {getRawValueAs<nautilus::val<int32_t>>(), nullable, null};
        }
        case DataType::Type::INT64: {
            return {getRawValueAs<nautilus::val<int64_t>>(), nullable, null};
        }
        case DataType::Type::UINT8: {
            return {getRawValueAs<nautilus::val<uint8_t>>(), nullable, null};
        }
        case DataType::Type::UINT16: {
            return {getRawValueAs<nautilus::val<uint16_t>>(), nullable, null};
        }
        case DataType::Type::UINT32: {
            return {getRawValueAs<nautilus::val<uint32_t>>(), nullable, null};
        }
        case DataType::Type::UINT64: {
            return {getRawValueAs<nautilus::val<uint64_t>>(), nullable, null};
        }
        case DataType::Type::FLOAT32: {
            return {getRawValueAs<nautilus::val<float>>(), nullable, null};
        }
        case DataType::Type::FLOAT64: {
            return {getRawValueAs<nautilus::val<double>>(), nullable, null};
        }
        case DataType::Type::VARSIZED: {
            return {getRawValueAs<VariableSizedData>(), nullable, null};
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
            else if constexpr (requires(T::basic_type type) { os << (nautilus::val<typename T::basic_type>(type)); })
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

namespace
{
/// Raises the ArithmeticalError at query runtime, also from compiled code
template <bool IsModulo>
void raiseDivisorIsZero()
{
    nautilus::invoke(+[] { throw ArithmeticalError(IsModulo ? "Can not modulo by zero!" : "Can not divide by zero!"); });
}

template <bool IsModulo>
void raiseMinByMinusOne()
{
    nautilus::invoke(+[] { throw ArithmeticalError(IsModulo ? "Can not modulo MIN by -1!" : "Can not divide MIN by -1!"); });
}

/// Integer division and remainder trap in hardware (SIGFPE) on a zero divisor and on MIN / -1. Both must surface as a query error
/// instead of killing the worker. Returns the denominator that is safe to divide by:
///  - NOT NULL integers: throws ArithmeticalError for a zero divisor and for MIN / -1, otherwise returns the divisor unchanged.
///  - nullable integers: throws for a non-null zero divisor and for non-null MIN / -1. A NULL operand holds arbitrary raw bits, so
///    the divisor is replaced by 1 whenever the raw operands would trap, and the (discarded) division can not fault.
///  - other operand types (floating point): a zero divisor throws only for nullable operands, as before.
template <bool IsModulo, typename LHS, typename RHS>
RHS makeSafeDenominator(
    const LHS& lhsVal,
    const RHS& rhsVal,
    const bool nullable,
    const nautilus::val<bool>& leftIsNull,
    const nautilus::val<bool>& rightIsNull)
{
    using LhsBasic = typename LHS::basic_type;
    using RhsBasic = typename RHS::basic_type;
    if constexpr (not(std::is_integral_v<LhsBasic> and std::is_integral_v<RhsBasic>))
    {
        if (nullable)
        {
            if (rhsVal == RHS{0} and not rightIsNull)
            {
                raiseDivisorIsZero<IsModulo>();
            }
            return nautilus::select(rhsVal == RHS{0}, RHS{1}, rhsVal);
        }
        return rhsVal;
    }
    else
    {
        /// MIN / -1 needs two signed operands, the dividend being the minimum of the (promoted) result type. Operands narrower than int are
        /// promoted to int, so e.g. INT8_MIN / -1 = 128 is representable and does not trap. With an unsigned operand the result is unsigned.
        using ResultBasic = typename decltype(lhsVal / rhsVal)::basic_type;
        constexpr bool canOverflow = std::is_signed_v<LhsBasic> and std::is_signed_v<RhsBasic> and std::is_signed_v<ResultBasic>
            and std::numeric_limits<LhsBasic>::min() == std::numeric_limits<ResultBasic>::min();

        const auto isZero = rhsVal == RHS{0};
        if (nullable)
        {
            if (isZero and not rightIsNull)
            {
                raiseDivisorIsZero<IsModulo>();
            }
            if constexpr (canOverflow)
            {
                const auto isOverflow = lhsVal == LHS{std::numeric_limits<LhsBasic>::min()} and rhsVal == RHS{-1};
                if (isOverflow and not(leftIsNull or rightIsNull))
                {
                    raiseMinByMinusOne<IsModulo>();
                }
                return nautilus::select(isZero or isOverflow, RHS{1}, rhsVal);
            }
            else
            {
                return nautilus::select(isZero, RHS{1}, rhsVal);
            }
        }

        if (isZero)
        {
            raiseDivisorIsZero<IsModulo>();
        }
        if constexpr (canOverflow)
        {
            if (lhsVal == LHS{std::numeric_limits<LhsBasic>::min()} and rhsVal == RHS{-1})
            {
                raiseMinByMinusOne<IsModulo>();
            }
        }
        return rhsVal;
    }
}
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
                const auto safeDenominator
                    = makeSafeDenominator<false>(lhsVal, rhsVal, leftIsNullable or rightIsNullable, leftIsNull, rightIsNull);
                if (leftIsNullable or rightIsNullable)
                {
                    using ResultType = decltype(lhsVal / rhsVal);
                    const auto newValue = nautilus::select(leftIsNull or rightIsNull, ResultType{0}, lhsVal / safeDenominator);
                    return VarVal{newValue, true, leftIsNull or rightIsNull};
                }
                return VarVal{lhsVal / safeDenominator, false, false};
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
                const auto safeDenominator
                    = makeSafeDenominator<true>(lhsVal, rhsVal, leftIsNullable or rightIsNullable, leftIsNull, rightIsNull);
                if (leftIsNullable or rightIsNullable)
                {
                    using ResultType = decltype(lhsVal % rhsVal);
                    const auto newValue = nautilus::select(leftIsNull or rightIsNull, ResultType{0}, lhsVal % safeDenominator);
                    return VarVal{newValue, true, leftIsNull or rightIsNull};
                }
                return VarVal{lhsVal % safeDenominator, false, false};
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
