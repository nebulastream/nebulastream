//
// Created by ls on 4/23/25.
//

#pragma once
#include <vector>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Util/Overloaded.hpp>
#include <val_concepts.hpp>

struct PhysicalType
{
    virtual ~PhysicalType() = default;
    virtual std::vector<NES::Nautilus::VarVal> store() const = 0;
    virtual void load(std::vector<NES::Nautilus::VarVal>) = 0;
};

template <typename... Ts>
std::variant<nautilus::val<Ts>...> fundamental(NES::Nautilus::FundamentalScalarValue val)
{
    return std::visit(
        NES::Overloaded{
            [](NES::Nautilus::FundamentalBasicScalarValue& scalar) -> std::variant<nautilus::val<Ts>...>
            {
                return std::visit(
                    []<typename T1>(T1 t) -> std::variant<nautilus::val<Ts>...>
                    {
                        if constexpr (std::constructible_from<std::variant<nautilus::val<Ts>...>, T1>)
                        {
                            return t;
                        }
                        else
                        {
                            throw std::runtime_error("Expected T got something else");
                        }
                    },
                    scalar.value);
            },
            [](NES::Nautilus::Nullable&) -> std::variant<nautilus::val<Ts>...>
            { throw std::runtime_error("Expected T got something nullable"); },
        },
        val.value);
}

template <typename... Ts>
std::variant<nautilus::val<Ts>...> expect(NES::Nautilus::VarVal val)
{
    return std::visit(
        NES::Overloaded{
            [](const NES::Nautilus::FundamentalScalarValue& scalar) { return fundamental<Ts...>(scalar); },
            [](auto) -> std::variant<nautilus::val<Ts>...> { throw std::runtime_error("Expected T got something else"); },
        },
        val.value);
}

template <typename T>
nautilus::val<T> expect_s(NES::Nautilus::VarVal val)
{
    return std::get<nautilus::val<T>>(expect<T>(val));
}

struct Integer : PhysicalType
{
    std::vector<NES::Nautilus::VarVal> store() const override
    {
        return std::vector{
            std::visit([](const auto& value) -> NES::Nautilus::VarVal { return NES::Nautilus::FundamentalScalarValue{value}; }, value)};
    }
    void load(std::vector<NES::Nautilus::VarVal> values) override
    {
        INVARIANT(values.size() == 1, "Should be one");
        value = expect<int8_t, int32_t>(values[0]);
    }

    std::variant<nautilus::val<int8_t>, nautilus::val<int32_t>> value;
    explicit Integer(const std::variant<nautilus::val<int8_t>, nautilus::val<int32_t>>& value) : value(value) { }
    Integer() = default;
};


template <typename R, typename T>
nautilus::val<R> cast_or_throw(nautilus::val<T> in)
{
    if constexpr (std::convertible_to<nautilus::val<T>, nautilus::val<R>>)
    {
        return static_cast<nautilus::val<R>>(in);
    }
    else
    {
        throw std::runtime_error("Expected type to be convertible to");
    }
}

Integer Add(Integer lhs, Integer rhs)
{
    nautilus::val<int8_t> lhs_val = std::visit([](const auto& st) { return cast_or_throw<int8_t>(st); }, lhs.value);
    nautilus::val<int8_t> rhs_val = std::visit([](const auto& st) { return cast_or_throw<int8_t>(st); }, rhs.value);

    return Integer{lhs_val + rhs_val};
}