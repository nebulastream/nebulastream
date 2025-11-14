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

#pragma once

#include <cstdint>
#include <utility>
#include <variant>
#include <DataTypes/DataType.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <Util/Logger/Logger.hpp>
#include <nautilus/std/ostream.h>
#include <nautilus/std/sstream.h>
#include <nautilus/std/string.h>
#include <nautilus/val.hpp>
#include <nautilus/val_ptr.hpp>
#include <ErrorHandling.hpp>
#include <val_concepts.hpp>

namespace NES
{
#define DEFINE_OPERATOR_VAR_VAL_BINARY(operatorName, op) \
    VarVal operatorName(const VarVal& rhs) const \
    { \
        return std::visit( \
            [leftIsNullable = this->isNullable(), \
             rightIsNullable = rhs.isNullable(), \
             leftIsNull = this->isNull(), \
             rightIsNull = rhs.isNull()]<typename LHS, typename RHS>(const LHS& lhsVal, const RHS& rhsVal) \
            { \
                if constexpr (requires(LHS l, RHS r) { l op r; }) \
                { \
                    auto newIsNullable = VarVal::NULLABLE_ENUM::NOT_NULLABLE; \
                    if (leftIsNullable or rightIsNullable) \
                    { \
                        newIsNullable = VarVal::NULLABLE_ENUM::NULLABLE; \
                    } \
                    const nautilus::val<bool> newNullValue = static_cast<nautilus::val<int>>(leftIsNullable & leftIsNull) \
                        | static_cast<nautilus::val<int>>(rightIsNullable & rightIsNull); \
                    return VarVal{lhsVal op rhsVal, newIsNullable, newNullValue}; \
                } \
                else \
                { \
                    throw UnknownOperation( \
                        std::string("VarVal operation not implemented: ") + " " + #operatorName + " " + typeid(LHS).name() + " " \
                        + typeid(RHS).name()); \
                    return VarVal{lhsVal, VarVal::NULLABLE_ENUM::NULLABLE, true}; \
                } \
            }, \
            this->value, \
            rhs.value); \
    }

#define DEFINE_OPERATOR_VAR_VAL_UNARY(operatorName, op) \
    VarVal operatorName() const \
    { \
        return std::visit( \
            [isNullable = isNullable(), isNull = isNull()]<typename RHS>(const RHS& rhsVal) \
            { \
                if constexpr (!requires(RHS r) { op r; }) \
                { \
                    throw UnknownOperation( \
                        std::string("VarVal operation not implemented: ") + " " + #operatorName + " " + typeid(decltype(rhsVal)).name()); \
                    return VarVal{detail::var_val_t(rhsVal), VarVal::NULLABLE_ENUM::NULLABLE, false}; \
                } \
                else \
                { \
                    auto newIsNullable = VarVal::NULLABLE_ENUM::NOT_NULLABLE; \
                    if (isNullable) \
                    { \
                        newIsNullable = VarVal::NULLABLE_ENUM::NULLABLE; \
                    } \
                    nautilus::val<bool> newNullValue = isNullable & isNull; \
                    detail::var_val_t result = op rhsVal; \
                    return VarVal{detail::var_val_t(result), newIsNullable, newNullValue}; \
                } \
            }, \
            this->value); \
    }

namespace detail
{
template <typename... T>
using var_val_helper = std::variant<VariableSizedData, nautilus::val<T>...>;
using var_val_t = var_val_helper<bool, uint8_t, uint16_t, uint32_t, uint64_t, int8_t, int16_t, int32_t, int64_t, float, double, char>;


/// Lookup if a T is in a Variant
template <class T, class U>
struct is_one_of;

template <class T, class... Ts>
struct is_one_of<T, std::variant<Ts...>> : std::bool_constant<(std::is_same_v<T, Ts> || ...)>
{
};
}

/// This class is the base class for all data types in our Nautilus-Backend.
/// It sits on top of the nautilus library and its val<> data type.
/// We derive all specific data types, e.g., variable and fixed data types, from this base class.
/// This class provides all functionality so that a developer does not have to know of any nautilus::val<> and how to work with them.
class VarVal
{
public:
    enum class NULLABLE_ENUM : uint8_t
    {
        NOT_NULLABLE = 0,
        NULLABLE = 1,
    };

    /// Construct a VarVal object from memory
    static VarVal readVarValFromMemory(const nautilus::val<int8_t*>& memRef, DataType type);
    static VarVal readVarValFromMemory(const nautilus::val<int8_t*>& memRef, DataType type, const nautilus::val<bool>& null);

    /// Construct a VarVal object for example via VarVal(32)
    template <typename T>
    explicit VarVal(const T t, const NULLABLE_ENUM nullable = NULLABLE_ENUM::NOT_NULLABLE, const nautilus::val<bool>& null = false)
    requires(detail::is_one_of<nautilus::val<T>, detail::var_val_t>::value)
        : value(nautilus::val<T>(t)), null(null), nullable(nullable)
    {
    }

    /// Construct via VarVal(nautilus::val<int>(32)), also allows conversion from static to dynamic
    template <typename T>
    VarVal(const nautilus::val<T> t, const NULLABLE_ENUM nullable = NULLABLE_ENUM::NOT_NULLABLE, const nautilus::val<bool>& null = false)
    requires(detail::is_one_of<nautilus::val<T>, detail::var_val_t>::value)
        : value(t), null(null), nullable(nullable)
    {
    }

    /// Construct a VarVal object for all other types that are part of var_val_helper but are not wrapped
    /// in a nautilus::val<> can be constructed via this constructor, e.g, VarVal(VariableSize).
    template <typename T>
    requires(detail::is_one_of<T, detail::var_val_t>::value)
    VarVal(const T t, const NULLABLE_ENUM nullable = NULLABLE_ENUM::NOT_NULLABLE, const nautilus::val<bool>& null = false)
        : value(t), null(null), nullable(nullable)
    {
    }

    VarVal(const VarVal& other);
    VarVal(VarVal&& other) noexcept;
    VarVal& operator=(const VarVal& other);
    VarVal& operator=(VarVal&& other); /// NOLINT, as we need to have the option of throwing
    explicit operator bool() const;
    friend nautilus::val<std::ostream>& operator<<(nautilus::val<std::ostream>& os, const VarVal& varVal);

    /// Casts the underlying value to the given type T1. castToType() or cast<T>() should be the only way how the underlying value can be accessed.
    template <typename T1>
    T1 cast() const
    {
        /// If the underlying value is the same type as T1, we can return it directly.
        if (std::holds_alternative<T1>(value))
        {
            return std::get<T1>(value);
        }

        return std::visit(
            []<typename T0>(T0&& underlyingValue) -> T1
            {
                using removedCVRefT0 = std::remove_cvref_t<T0>;
                using removedCVRefT1 = std::remove_cvref_t<T1>;
                if constexpr (std::is_same_v<removedCVRefT0, VariableSizedData> || std::is_same_v<removedCVRefT1, VariableSizedData>)
                {
                    throw UnknownOperation("Cannot cast VariableSizedData to anything else.");
                }
                else
                {
                    return static_cast<T1>(underlyingValue);
                }
            },
            value);
    }

    /// Casts the underlying value to the provided type. castToType() or cast<T>() should be the only way how the underlying value can be accessed.
    [[nodiscard]] VarVal castToType(DataType::Type type) const;

    template <typename T>
    VarVal customVisit(T t) const
    {
        return std::visit(t, value);
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

    /// With div we might have a problem if we divide by 0 even though the VarVal is null. To handle this special case,
    /// we create a custom method for this and do not use the #define for the other binary operators
    VarVal operator/(const VarVal& rhs) const
    {
        return std::visit(
            [leftIsNullable = this->isNullable(),
             rightIsNullable = rhs.isNullable(),
             leftIsNull = this->isNull(),
             rightIsNull = rhs.isNull()]<typename LHS, typename RHS>(const LHS& lhsVal, const RHS& rhsVal)
            {
                if constexpr (requires(LHS l, RHS r) { l / r; })
                {
                    auto newIsNullable = VarVal::NULLABLE_ENUM::NOT_NULLABLE;
                    if (leftIsNullable or rightIsNullable)
                    {
                        newIsNullable = VarVal::NULLABLE_ENUM::NULLABLE;
                    }
                    /// We use an val<int> to use bitwise operators to reduce the number of branches created in the nautilus trace.
                    const auto leftIsNullableInt = static_cast<int>(leftIsNullable);
                    const auto rightIsNullableInt = static_cast<int>(rightIsNull);
                    const auto leftIsNullInt = static_cast<nautilus::val<int>>(leftIsNull);
                    const auto rightIsNullInt = static_cast<nautilus::val<int>>(rightIsNull);
                    const nautilus::val<bool> newNullValue = leftIsNullableInt & leftIsNullInt | rightIsNullableInt & rightIsNullInt;
                    const nautilus::val<bool> isZeroDenominator
                        = static_cast<nautilus::val<int>>(rhsVal == 0) & (rightIsNullableInt & rightIsNullInt);
                    /// Using safe denominator if it is zero and rhs is null
                    return VarVal{lhsVal / (rhsVal + isZeroDenominator), newIsNullable, newNullValue};
                }
                else
                {
                    throw UnknownOperation(
                        std::string("VarVal operation not implemented: ") + " " + +" " + typeid(LHS).name() + " " + typeid(RHS).name());
                    return VarVal{lhsVal, VarVal::NULLABLE_ENUM::NULLABLE, true};
                }
            },
            this->value,
            rhs.value);
    }

    /// Writes the underlying value to the given memory reference.
    /// We call the operator= after the cast to the underlying type.
    /// This method does NOT take care of writing a potential null value, as this is taken care in the TupleBufferRef.
    void writeToMemory(const nautilus::val<int8_t*>& memRef) const;
    [[nodiscard]] nautilus::val<bool> isNull() const;
    [[nodiscard]] bool isNullable() const;

protected:
    /// ReSharper disable once CppNonExplicitConvertingConstructor
    explicit VarVal(const detail::var_val_t t, const NULLABLE_ENUM nullable = NULLABLE_ENUM::NOT_NULLABLE, nautilus::val<bool> null = false)
        : value(t), null(std::move(null)), nullable(nullable)
    {
    }

    detail::var_val_t value;
    nautilus::val<bool> null;
    NULLABLE_ENUM nullable; /// Allows us to not run the null code parts, if the VarVal is not nullable
};

static_assert(!std::is_default_constructible_v<VarVal>, "Should not be default constructible");
static_assert(std::is_constructible_v<VarVal, int32_t>, "Should be constructible from int32_t");
static_assert(
    std::is_constructible_v<VarVal, int32_t, VarVal::NULLABLE_ENUM, bool>,
    "Should be constructible from int32_t, VarVal::NULLABLE_ENUM and bool");
static_assert(!std::is_constructible_v<VarVal, int32_t, bool>, "Should not be constructible from int32_t and bool");
static_assert(
    std::is_constructible_v<VarVal, VariableSizedData, VarVal::NULLABLE_ENUM, bool>,
    "Should be constructible from VariableSizedData, VarVal::NULLABLE_ENUM and bool");
static_assert(
    std::is_constructible_v<VarVal, nautilus::val<uint32_t>, VarVal::NULLABLE_ENUM, nautilus::val<bool>>,
    "Should be constructible from nautilus::val<uint32_t> and nautilus::val<bool>");
static_assert(std::is_convertible_v<nautilus::val<uint32_t>, VarVal>, "Should not allow conversion from nautilus::val<uint32_t> to VarVal");
static_assert(
    std::is_constructible_v<VarVal, VariableSizedData, VarVal::NULLABLE_ENUM, nautilus::val<bool>>,
    "Should be constructible from VariableSizedData");
static_assert(std::is_convertible_v<VariableSizedData, VarVal>, "Should allow conversion from VariableSizedData to VarVal");
static_assert(!std::is_convertible_v<int32_t, VarVal>, "Should not allow conversion from underlying to VarVal");

nautilus::val<std::ostream>& operator<<(nautilus::val<std::ostream>& os, const VarVal& varVal);
}
