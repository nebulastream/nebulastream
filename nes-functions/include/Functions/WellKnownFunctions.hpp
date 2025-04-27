//
// Created by ls on 4/21/25.
//

#pragma once
#include <string_view>
#include <unordered_set>

namespace NES::WellKnown
{
constexpr static std::string_view Equal = "Equal";
constexpr static std::string_view NotEqual = "NotEqual";
constexpr static std::string_view Less = "Less";
constexpr static std::string_view Greater = "Greater";
constexpr static std::string_view LessEqual = "LessEqual";
constexpr static std::string_view GreaterEqual = "GreaterEqual";
constexpr static std::string_view Negate = "Negate";
constexpr static std::string_view Add = "Add";
constexpr static std::string_view Mul = "Mul";
constexpr static std::string_view Sub = "Sub";
constexpr static std::string_view Div = "Div";
constexpr static std::string_view Mod = "Mod";
constexpr static std::string_view And = "And";
constexpr static std::string_view Or = "Or";

constexpr static std::string_view Pow = "Pow";
constexpr static std::string_view Abs = "Abs";
constexpr static std::string_view Sqrt = "Sqrt";
constexpr static std::string_view Exp = "Exp";
constexpr static std::string_view Round = "Round";
constexpr static std::string_view Ceil = "Ceil";
constexpr static std::string_view Floor = "Floor";

const static std::unordered_set<std::string_view> EqualityFunctions = {Equal, NotEqual, Less, Greater, LessEqual, GreaterEqual};
};
