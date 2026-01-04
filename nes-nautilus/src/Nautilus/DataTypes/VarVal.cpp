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
#include <nautilus/std/ostream.h>
#include <nautilus/val.hpp>
#include <nautilus/val_ptr.hpp>
#include <ErrorHandling.hpp>
#include <nameof.hpp>
#include <select.hpp>
#include <val_concepts.hpp>

namespace NES
{

VarVal::VarVal(const VarVal& other) : value(other.value), null(other.null), nullable(other.nullable)
{
}

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
    return nullable == NULLABLE_ENUM::NULLABLE;
}

VarVal::operator bool() const
{
    const nautilus::val<bool> valueBool = std::visit(
        []<typename T>(T& val) -> nautilus::val<bool>
        {
            if constexpr (requires(T t) { t == nautilus::val<bool>(true); })
            {
                return val == nautilus::val<bool>(true);
            }
            else
            {
                throw UnknownOperation();
            }
        },
        value);

    if (isNullable())
    {
        return !isNull() && valueBool;
    }
    return valueBool;
}

VarVal VarVal::castToType(const DataType::Type type) const
{
    switch (type)
    {
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
        case DataType::Type::VARSIZED_POINTER_REP:
            throw UnknownDataType(
                "Not supporting reading {} data type from memory. VARSIZED_POINTER_REP should is only supported in the ChainedHashMap!",
                magic_enum::enum_name(type));
        case DataType::Type::CHAR:
        case DataType::Type::UNDEFINED:
            throw UnknownDataType("Not supporting reading {} data type from memory.", magic_enum::enum_name(type));
    }
    std::unreachable();
}

VarVal VarVal::readVarValFromMemory(const nautilus::val<int8_t*>& memRef, const DataType type)
{
    PRECONDITION(
        not type.isNullable,
        "This function can only be called if the data type is not nullable. Please use the overloaded function readVarValFromMemory(const "
        "nautilus::val<int8_t*>&, DataType, const nautilus::val<bool>&) instead!");
    return VarVal::readVarValFromMemory(memRef, type, nautilus::val<bool>(false));
}

VarVal VarVal::readVarValFromMemory(const nautilus::val<int8_t*>& memRef, const DataType type, const nautilus::val<bool>& null)
{
    switch (type.type)
    {
        case DataType::Type::BOOLEAN: {
            return {readValueFromMemRef<bool>(memRef), type.isNullable ? NULLABLE_ENUM::NULLABLE : NULLABLE_ENUM::NOT_NULLABLE, null};
        }
        case DataType::Type::INT8: {
            return {readValueFromMemRef<int8_t>(memRef), type.isNullable ? NULLABLE_ENUM::NULLABLE : NULLABLE_ENUM::NOT_NULLABLE, null};
        }
        case DataType::Type::INT16: {
            return {readValueFromMemRef<int16_t>(memRef), type.isNullable ? NULLABLE_ENUM::NULLABLE : NULLABLE_ENUM::NOT_NULLABLE, null};
        }
        case DataType::Type::INT32: {
            return {readValueFromMemRef<int32_t>(memRef), type.isNullable ? NULLABLE_ENUM::NULLABLE : NULLABLE_ENUM::NOT_NULLABLE, null};
        }
        case DataType::Type::INT64: {
            return {readValueFromMemRef<int64_t>(memRef), type.isNullable ? NULLABLE_ENUM::NULLABLE : NULLABLE_ENUM::NOT_NULLABLE, null};
        }
        case DataType::Type::CHAR: {
            return {readValueFromMemRef<char>(memRef), type.isNullable ? NULLABLE_ENUM::NULLABLE : NULLABLE_ENUM::NOT_NULLABLE, null};
        }
        case DataType::Type::UINT8: {
            return {readValueFromMemRef<uint8_t>(memRef), type.isNullable ? NULLABLE_ENUM::NULLABLE : NULLABLE_ENUM::NOT_NULLABLE, null};
        }
        case DataType::Type::UINT16: {
            return {readValueFromMemRef<uint16_t>(memRef), type.isNullable ? NULLABLE_ENUM::NULLABLE : NULLABLE_ENUM::NOT_NULLABLE, null};
        }
        case DataType::Type::UINT32: {
            return {readValueFromMemRef<uint32_t>(memRef), type.isNullable ? NULLABLE_ENUM::NULLABLE : NULLABLE_ENUM::NOT_NULLABLE, null};
        }
        case DataType::Type::UINT64: {
            return {readValueFromMemRef<uint64_t>(memRef), type.isNullable ? NULLABLE_ENUM::NULLABLE : NULLABLE_ENUM::NOT_NULLABLE, null};
        }
        case DataType::Type::FLOAT32: {
            return {readValueFromMemRef<float>(memRef), type.isNullable ? NULLABLE_ENUM::NULLABLE : NULLABLE_ENUM::NOT_NULLABLE, null};
        }
        case DataType::Type::FLOAT64: {
            return {readValueFromMemRef<double>(memRef), type.isNullable ? NULLABLE_ENUM::NULLABLE : NULLABLE_ENUM::NOT_NULLABLE, null};
        }
        case DataType::Type::VARSIZED:
        case DataType::Type::VARSIZED_POINTER_REP:
        case DataType::Type::UNDEFINED:
            throw UnknownDataType("Not supporting reading {} data type from memory.", magic_enum::enum_name(type.type));
    }
    std::unreachable();
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

VarVal varValSelect(const nautilus::val<bool>& condition, const VarVal& trueValue, const VarVal& falseValue)
{
    return std::visit(
        [&]<typename LHS, typename RHS>(const LHS& trueUnderlying, const RHS& falseUnderlying) -> VarVal
        {
            if constexpr (std::same_as<LHS, RHS> && !std::same_as<LHS, VariableSizedData>)
            {
                return VarVal(nautilus::select(condition, trueUnderlying, falseUnderlying));
            }

            if constexpr (std::same_as<LHS, RHS> && std::same_as<LHS, VariableSizedData>)
            {
                return VarVal(VariableSizedData(
                    nautilus::select(condition, trueUnderlying.getReference(), falseUnderlying.getReference()),
                    nautilus::select(condition, trueUnderlying.getContentSize(), falseUnderlying.getContentSize())));
            }
            throw UnknownOperation("select with different types! True: {} vs. False: {}", NAMEOF_TYPE(LHS), NAMEOF_TYPE(RHS));
            std::unreachable();
        },
        trueValue.value,
        falseValue.value);
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
                        const nautilus::val<bool> newNullValue = static_cast<nautilus::val<int>>(leftIsNullable & leftIsNull) \
                            | static_cast<nautilus::val<int>>(rightIsNullable & rightIsNull); \
                        return VarVal{lhsVal op rhsVal, VarVal::NULLABLE_ENUM::NULLABLE, newNullValue}; \
                    } \
                    return VarVal{lhsVal op rhsVal, VarVal::NULLABLE_ENUM::NOT_NULLABLE, nautilus::val<bool>(false)}; \
                } \
                else \
                { \
                    throw UnknownOperation("VarVal operation not implemented: {} " #op " {}", NAMEOF_TYPE(LHS), NAMEOF_TYPE(RHS)); \
                    return VarVal{lhsVal, VarVal::NULLABLE_ENUM::NULLABLE, true}; \
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
                    return VarVal{detail::var_val_t(rhsVal), VarVal::NULLABLE_ENUM::NULLABLE, false}; \
                } \
                else \
                { \
                    if (isNullable) \
                    { \
                        return VarVal{detail::var_val_t(op rhsVal), VarVal::NULLABLE_ENUM::NULLABLE, isNull}; \
                    } \
                    return VarVal{detail::var_val_t(op rhsVal), VarVal::NULLABLE_ENUM::NOT_NULLABLE, nautilus::val<bool>(false)}; \
                } \
            }, \
            this->value); \
    }

/// With div we might have a problem if we divide by 0 even though the VarVal is null. To handle this special case,
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
                    auto isNull = leftIsNull || rightIsNull;
                    auto result = nautilus::select(isNull, static_cast<decltype(lhsVal / rhsVal)>(lhsVal), lhsVal / rhsVal);
                    return VarVal{result, NULLABLE_ENUM::NULLABLE, isNull};
                }
                return VarVal{lhsVal / rhsVal, NULLABLE_ENUM::NULLABLE, nautilus::val<bool>(false)};
            }
            else
            {
                throw UnknownOperation(
                    std::string("VarVal operation not implemented: ") + " " + +" " + typeid(LHS).name() + " " + typeid(RHS).name());
                return VarVal{lhsVal, VarVal::NULLABLE_ENUM::NULLABLE, true};
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
DEFINE_OPERATOR_VAR_VAL_BINARY(operator%, %);
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
