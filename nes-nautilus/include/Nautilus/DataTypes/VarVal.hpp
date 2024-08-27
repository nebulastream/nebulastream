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

#ifndef NES_NAUTILUS_INCLUDE_NAUTILUS_DATATYPES_VARVAL_HPP_
#define NES_NAUTILUS_INCLUDE_NAUTILUS_DATATYPES_VARVAL_HPP_

#include <Common/PhysicalTypes/PhysicalType.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <Util/Logger/Logger.hpp>
#include <nautilus/val.hpp>
#include <nautilus/val_ptr.hpp>
#include <variant>

namespace NES::Nautilus {
#define DEFINE_BINARY_OPERATOR_VAR_VAL(operatorName, op)                                                                                 \
    VarVal operatorName(const VarVal& rhs) const {                                                                               \
        return std::visit(                                                                                                       \
            [&]<typename LHS, typename RHS>(const LHS& lhsVal, const RHS& rhsVal) {                                              \
                if constexpr (requires(LHS l, RHS r) { l op r; }) {                                                              \
                    detail::var_val_t result = (lhsVal op rhsVal);                                                               \
                    return result;                                                                                               \
                } else {                                                                                                         \
                    throw std::runtime_error(std::string("VarVal operation not implemented: ") + " " + #operatorName + " "       \
                                             + typeid(LHS).name() + " " + typeid(RHS).name());                                   \
                    return detail::var_val_t(lhsVal);                                                                            \
                }                                                                                                                \
            },                                                                                                                   \
            this->value,                                                                                                         \
            rhs.value);                                                                                                          \
    }

#define DEFINE_UNARY_OPERATOR_VAR_VAL(operatorName, op)                                                                                  \
    VarVal operatorName() const {                                                                                                \
        return std::visit(                                                                                                       \
            [&]<typename RHS>(const RHS& rhsVal) {                                                                                            \
                if constexpr (!requires(RHS r) { op r; }) {                                                                        \
                    throw std::runtime_error(std::string("VarVal operation not implemented: ") + " " + #operatorName + " "       \
                                             + typeid(decltype(rhsVal)).name());                                                 \
                    return detail::var_val_t(rhsVal);                                                                            \
                } else {                                                                                                         \
                    detail::var_val_t result = op rhsVal;                                                                        \
                    return result;                                                                                               \
                }                                                                                                                \
            },                                                                                                                   \
            this->value);                                                                                                        \
    }

#define EVALUATE_FUNCTION(func)                                                                                                  \
    [&](const auto& val) {                                                                                                       \
        if constexpr (!requires { func(val); }) {                                                                                \
            throw std::runtime_error(std::string("VarVal function not implemented: ") + typeid(decltype(val)).name());           \
            return NES::Nautilus::detail::var_val_t(val);                                                                        \
        } else {                                                                                                                 \
            NES::Nautilus::detail::var_val_t result = func(val);                                                                 \
            return result;                                                                                                       \
        }                                                                                                                        \
    }

namespace detail {
template<typename... T>
using var_val_helper = std::variant<VariableSizedData, nautilus::val<T>...>;
using var_val_t = var_val_helper<bool, uint8_t, uint16_t, uint32_t, uint64_t, int8_t, int16_t, int32_t, int64_t, float, double>;

// Lookup if a T is in a Variant
template<class T, class U>
struct is_one_of;
template<class T, class... Ts>
struct is_one_of<T, std::variant<Ts...>> : std::bool_constant<(std::is_same_v<T, Ts> || ...)> {};
}// namespace detail

/// This class is the base class for all data types in NebulaStream.
/// It sits on top of the nautilus library and its val<> data type.
/// We derive all specific data types, e.g., variable and fixed data types, from this base class.
/// This class provides all functionality so that a developer does not have to know of any nautilus::val<> and how to work with them.
///
/// As we want to support null handling, this class has a `val<bool> null` member.
/// Even though, we have here a null member, we do not support end-to-end null handling.
/// All operations on VarVals are implemented with null handling in mind, but
/// we currently do not check for potential null values in any operator.
class VarVal {
  public:
    /// Construct a VarVal object via VarVal(32)
    template<typename T>
    explicit VarVal(const T t, const bool isNull = false)
        requires(detail::is_one_of<nautilus::val<T>, detail::var_val_t>::value)
        : value(nautilus::val<T>(t)), null(isNull) {}

    /// Construct via VarVal(nautilus::val<int>(32)), also allows conversion from static to dynamic
    template<typename T>
    VarVal(const nautilus::val<T> t, const nautilus::val<bool> isNull = false)
        requires(detail::is_one_of<nautilus::val<T>, detail::var_val_t>::value)
        : value(t), null(isNull) {}

    /// Construct a VarVal object for all other types that are part of var_val_helper but are not wrapped
    /// in a nautilus::val<> can be constructed via this constructor, e.g, VarVal(VariableSize).
    template<typename T>
    VarVal(const T t, const nautilus::val<bool> isNull = false)
        requires(detail::is_one_of<T, detail::var_val_t>::value)
        : value(t), null(isNull) {}

    VarVal(const VarVal& other);
    VarVal(VarVal&& other) noexcept;
    VarVal& operator=(const VarVal& other);
    VarVal& operator=(VarVal&& other);

    /// This is only for the PoC. We should think when do we need this and if we can get rid of the usages.
    template<typename T>
    [[nodiscard]] bool holdsVariant() const {
        if (std::holds_alternative<T>(value)) {
            return true;
        }
        return false;
    }

    /// Casts the underlying value to the given type T1. If the cast is not possible, a std::bad_variant_access exception is thrown.
    /// This should be the only way how the underlying value can be accessed.
    template<typename T1>
    T1 cast() const {
        if (std::holds_alternative<T1>(value)) {
            return std::get<T1>(value);
        }
        throw std::bad_variant_access();
    }

    template<typename T>
    VarVal customVisit(T t) const {
        return std::visit(t, value);
    }

    /// Defining operations on VarVal. In the macro, we use std::variant and std::visit to automatically call the already
    /// existing operations on the underlying nautilus::val<> data types.
    /// For the VarSizedDataType, we can define custom operations in the class itself.
    DEFINE_BINARY_OPERATOR_VAR_VAL(operator+, +);
    DEFINE_BINARY_OPERATOR_VAR_VAL(operator-, -);
    DEFINE_BINARY_OPERATOR_VAR_VAL(operator*, *);
    DEFINE_BINARY_OPERATOR_VAR_VAL(operator/, /);
    DEFINE_BINARY_OPERATOR_VAR_VAL(operator==, ==);
    DEFINE_BINARY_OPERATOR_VAR_VAL(operator!=, !=);
    DEFINE_BINARY_OPERATOR_VAR_VAL(operator&&, &&);
    DEFINE_BINARY_OPERATOR_VAR_VAL(operator||, ||);
    DEFINE_BINARY_OPERATOR_VAR_VAL(operator<, <);
    DEFINE_BINARY_OPERATOR_VAR_VAL(operator>, >);
    DEFINE_BINARY_OPERATOR_VAR_VAL(operator<=, <=);
    DEFINE_BINARY_OPERATOR_VAR_VAL(operator>=, >=);
    DEFINE_BINARY_OPERATOR_VAR_VAL(operator&, &);
    DEFINE_BINARY_OPERATOR_VAR_VAL(operator|, |);
    DEFINE_BINARY_OPERATOR_VAR_VAL(operator^, ^);
    DEFINE_BINARY_OPERATOR_VAR_VAL(operator<<, <<);
    DEFINE_BINARY_OPERATOR_VAR_VAL(operator>>, >>);
    DEFINE_UNARY_OPERATOR_VAR_VAL(operator!, !);

    /// Writes the underlying value to the given memory reference.
    /// We call the operator= after the cast to the underlying type.
    void writeVarValToMemRefVal(const MemRefVal& memRef) const;

    explicit operator bool() const;
    [[nodiscard]] const nautilus::val<bool>& isNull() const;

    friend std::ostream& operator<<(std::ostream& os, const VarVal& varVal);

  protected:
    // Conversion from underlying variant
    VarVal(const detail::var_val_t& t) : value(t), null(false) {}
    detail::var_val_t value;
    nautilus::val<bool> null;
};

static_assert(!std::is_default_constructible_v<VarVal>, "Should not be default constructible");
static_assert(std::is_constructible_v<VarVal, int32_t>, "Should be constructible from int32_t");
static_assert(std::is_constructible_v<VarVal, UInt32Val>, "Should be constructible from UInt32Val");
static_assert(std::is_convertible_v<UInt32Val, VarVal>, "Should allow conversion from UInt32Val to VarVal");
static_assert(std::is_constructible_v<VarVal, VariableSizedData>, "Should be constructible from VariableSizedData");
static_assert(std::is_convertible_v<VariableSizedData, VarVal>, "Should allow conversion from VariableSizedData to VarVal");
static_assert(!std::is_convertible_v<int32_t, VarVal>, "Should not allow conversion from underlying to VarVal");

/// A wrapper around std::memcmp() and checks if ptr1 and ptr2 are equal
BooleanVal memEquals(const MemRefVal& ptr1, const MemRefVal& ptr2, const UInt64Val& size);

/// A wrapper around std::memcpy() and copies size bytes from src to dest
void memCopy(const MemRefVal& dest, const MemRefVal& src, const nautilus::val<size_t>& size);

VarVal readVarValFromMemRef(const MemRefVal& memRef, const PhysicalTypePtr& type);

std::ostream& operator<<(std::ostream& os, const VarVal& varVal);
}// namespace NES::Nautilus

#endif// NES_NAUTILUS_INCLUDE_NAUTILUS_DATATYPES_VARVAL_HPP_
