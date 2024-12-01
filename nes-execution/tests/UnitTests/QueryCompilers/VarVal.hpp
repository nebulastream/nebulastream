//
// Created by ls on 11/30/24.
//

#pragma once

#include <cstddef>
#include <cstdint>
#include <unordered_set>
#include <variant>
#include <API/Schema.hpp>
#include <nautilus/Engine.hpp>
#include <nautilus/std/ostream.h>
#include <nautilus/std/sstream.h>
#include <nautilus/std/string.h>
#include <nautilus/val.hpp>
#include <nautilus/val_ptr.hpp>
#include <std/cstring.h>

#include <ErrorHandling.hpp>

template <typename T>
constexpr std::string_view typeName()
{
    std::string_view sv(__PRETTY_FUNCTION__);
    sv.remove_prefix(std::string_view("std::string_view typeName() [T = ").size());
    sv.remove_suffix(1);
    return sv;
}

namespace NES::Nautilus
{

#define EVALUATE_FUNCTION(func) \
    [&](const auto& val) \
    { \
        if constexpr (!requires { func(val); }) \
        { \
            throw UnsupportedOperation(std::string("ScalarVarVal function not implemented: ") + typeid(decltype(val)).name()); \
            return NES::Nautilus::detail::var_val_t(val); \
        } \
        else \
        { \
            NES::Nautilus::detail::var_val_t result = func(val); \
            return result; \
        } \
    }


namespace detail
{
template <typename... T>
using var_val_helper = std::variant<nautilus::val<T>...>;
using var_val_t = var_val_helper<bool, char, uint8_t, uint16_t, uint32_t, uint64_t, int8_t, int16_t, int32_t, int64_t, float, double>;
using var_val_ptr_t
    = var_val_helper<bool*, char*, uint8_t*, uint16_t*, uint32_t*, uint64_t*, int8_t*, int16_t*, int32_t*, int64_t*, float*, double*>;


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
class ScalarVarVal
{
public:
    /// Construct a ScalarVarVal object for example via ScalarVarVal(32)
    template <typename T>
    explicit ScalarVarVal(const T t)
    requires(detail::is_one_of<nautilus::val<T>, detail::var_val_t>::value)
        : value(nautilus::val<T>(t))
    {
    }

    /// Construct via ScalarVarVal(nautilus::val<int>(32)), also allows conversion from static to dynamic
    template <typename T>
    ScalarVarVal(const nautilus::val<T> t)
    requires(detail::is_one_of<nautilus::val<T>, detail::var_val_t>::value)
        : value(t)
    {
    }

    /// Construct a ScalarVarVal object for all other types that are part of var_val_helper but are not wrapped
    /// in a nautilus::val<> can be constructed via this constructor, e.g, ScalarVarVal(VariableSize).
    template <typename T>
    ScalarVarVal(const T t)
    requires(detail::is_one_of<T, detail::var_val_t>::value)
        : value(t)
    {
    }

    ScalarVarVal(const ScalarVarVal& other);
    ScalarVarVal(ScalarVarVal&& other) noexcept;
    ScalarVarVal& operator=(const ScalarVarVal& other);
    ScalarVarVal& operator=(ScalarVarVal&& other);
    explicit operator bool() const;
    friend nautilus::val<std::ostream>& operator<<(nautilus::val<std::ostream>& os, const ScalarVarVal& varVal);

    /// Casts the underlying value to the given type T1. If the cast is not possible, a std::bad_variant_access exception is thrown.
    /// This should be the only way how the underlying value can be accessed.
    template <typename T1>
    T1 cast() const
    {
        if (std::holds_alternative<T1>(value))
        {
            return std::get<T1>(value);
        }
        auto strType = std::visit([](auto&& x) -> decltype(auto) { return typeName<decltype(x)>(); }, value);
        throw UnsupportedOperation("Tried to cast to {} but was type {}", typeName<T1>(), strType);
    }

    template <typename T>
    ScalarVarVal customVisit(T t) const
    {
        return std::visit(t, value);
    }

    /// Defining operations on ScalarVarVal. In the macro, we use std::variant and std::visit to automatically call the already
    /// existing operations on the underlying nautilus::val<> data types.
    /// For the VarSizedDataType, we define custom operations in the class itself.

    ScalarVarVal operator+(const ScalarVarVal& rhs) const;
    ScalarVarVal operator-(const ScalarVarVal& rhs) const;
    ScalarVarVal operator*(const ScalarVarVal& rhs) const;
    ScalarVarVal operator/(const ScalarVarVal& rhs) const;
    ScalarVarVal operator==(const ScalarVarVal& rhs) const;
    ScalarVarVal operator!=(const ScalarVarVal& rhs) const;
    ScalarVarVal operator&&(const ScalarVarVal& rhs) const;
    ScalarVarVal operator||(const ScalarVarVal& rhs) const;
    ScalarVarVal operator<(const ScalarVarVal& rhs) const;
    ScalarVarVal operator>(const ScalarVarVal& rhs) const;
    ScalarVarVal operator<=(const ScalarVarVal& rhs) const;
    ScalarVarVal operator>=(const ScalarVarVal& rhs) const;
    ScalarVarVal operator<<(const ScalarVarVal& rhs) const;
    ScalarVarVal operator>>(const ScalarVarVal& rhs) const;
    ScalarVarVal operator&(const ScalarVarVal& rhs) const;
    ScalarVarVal operator!() const;
    ScalarVarVal operator^(const ScalarVarVal& rhs) const;
    ScalarVarVal operator|(const ScalarVarVal& rhs) const;


    /// Writes the underlying value to the given memory reference.
    /// We call the operator= after the cast to the underlying type.
    void writeToMemory(const nautilus::val<int8_t*>& memRef) const;

protected:
    /// ReSharper disable once CppNonExplicitConvertingConstructor
    ScalarVarVal(const detail::var_val_t t);

    detail::var_val_t value;
};


class ScalarVarValPtr
{
    detail::var_val_ptr_t value;

public:
    template <typename T>
    ScalarVarValPtr(const nautilus::val<T> t)
    requires(detail::is_one_of<nautilus::val<T>, detail::var_val_ptr_t>::value)
        : value(t)
    {
    }
    nautilus::val<bool> operator==(const ScalarVarValPtr& other) const;

    ScalarVarValPtr advanceBytes(nautilus::val<ssize_t> bytes) const;

    ScalarVarVal operator*();

    template <typename T>
    nautilus::val<T*> uncheckedCast() const
    {
        return std::visit([](auto&& x) { return static_cast<nautilus::val<T*>>(x); }, this->value);
    }

    template <typename T1>
    nautilus::val<T1*> cast() const
    {
        if (std::holds_alternative<nautilus::val<T1*>>(value))
        {
            return std::get<nautilus::val<T1*>>(value);
        }
        auto strType = std::visit([](auto&& x) -> decltype(auto) { return typeName<decltype(x)>(); }, value);
        throw UnsupportedOperation("Tried to cast to {} but was type {}", typeName<T1>(), strType);
    }

    template <typename T1>
    requires(std::same_as<void, T1>)
    nautilus::val<void*> cast() const
    {
        return std::visit([](auto&& x) { return static_cast<nautilus::val<void*>>(x); }, this->value);
    }
};


static_assert(!std::is_default_constructible_v<ScalarVarVal>, "Should not be default constructible");
static_assert(std::is_constructible_v<ScalarVarVal, int32_t>, "Should be constructible from int32_t");
static_assert(std::is_constructible_v<ScalarVarVal, nautilus::val<uint32_t>>, "Should be constructible from nautilus::val<uint32_t>");
static_assert(
    std::is_convertible_v<nautilus::val<uint32_t>, ScalarVarVal>, "Should allow conversion from nautilus::val<uint32_t> to ScalarVarVal");
static_assert(!std::is_convertible_v<int32_t, ScalarVarVal>, "Should not allow conversion from underlying to ScalarVarVal");

nautilus::val<std::ostream>& operator<<(nautilus::val<std::ostream>& os, const ScalarVarVal& varVal);

class FixedSizeVal
{
public:
    FixedSizeVal(std::vector<ScalarVarVal> values) : values(values), size(values.size()) { }
    std::vector<ScalarVarVal> values;
    size_t size;
    nautilus::val<bool> operator==(const FixedSizeVal& other) const
    {
        INVARIANT(this->size == other.size);
        return this->values == other.values;
    }
};

class VariableSizeVal
{
public:
    VariableSizeVal(ScalarVarValPtr data, nautilus::val<size_t> size, nautilus::val<size_t> numberOfElements);
    ScalarVarValPtr data;
    nautilus::val<size_t> size;
    nautilus::val<size_t> numberOfElements;

    nautilus::val<bool> operator==(const VariableSizeVal& other) const;
};

struct VarVal
{
    VarVal(ScalarVarVal scalar) : value(std::move(scalar)) { }
    VarVal(FixedSizeVal fixed_size_val) : value(std::move(fixed_size_val)) { }
    VarVal(VariableSizeVal fixed_size_val) : value(std::move(fixed_size_val)) { }

    ScalarVarVal& assumeScalar() { return std::get<ScalarVarVal>(value); }
    FixedSizeVal& assumeFixedSize() { return std::get<FixedSizeVal>(value); }
    VariableSizeVal& assumeVariableSize() { return std::get<VariableSizeVal>(value); }

     nautilus::val<bool> operator==(const VarVal& other) const;

    using impl = std::variant<ScalarVarVal, FixedSizeVal, VariableSizeVal>;
    impl value;
};
}
