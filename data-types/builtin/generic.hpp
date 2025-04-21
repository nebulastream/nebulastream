#pragma once
#include <string>
#include <string_view>

namespace NES
{

struct ConstantProperties
{
    std::string constantValue;
    bool operator==(const ConstantProperties&) const = default;
};

struct Constant : DataTypeImplHelper<Constant>
{
    explicit Constant(const ConstantProperties& properties) : properties(properties) { }
    static constexpr NameT Name = "CONSTANT";
    ConstantProperties properties;
    bool operator==(const Constant&) const = default;
};

struct BoolProperties
{
    std::optional<bool> value;
    bool operator==(const BoolProperties&) const = default;
};

struct Bool : DataTypeImplHelper<Bool>
{
    explicit Bool(const BoolProperties& properties) : properties(properties) { }
    constexpr static NameT Name = WellKnown::Bool;
    BoolProperties properties;
    bool operator==(const Bool&) const = default;
};
}