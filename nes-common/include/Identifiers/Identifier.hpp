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
#include <algorithm>
#include <cctype>
#include <functional>
#include <initializer_list>
#include <ranges>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <Util/Ranges.hpp>

namespace NES
{
class Identifier
{
    std::string value;
    bool caseSensitive{};

public:
    constexpr explicit Identifier() = default;
    constexpr explicit Identifier(const std::string_view value) : value(value) { }
    constexpr explicit Identifier(const std::string_view value, const bool caseSensitive) : value(value), caseSensitive(caseSensitive) { }
    constexpr Identifier(const Identifier& other) = default;
    constexpr Identifier(Identifier&& other) noexcept = default;
    constexpr Identifier& operator=(const Identifier& other) = default;
    constexpr Identifier& operator=(Identifier&& other) noexcept = default;
    constexpr ~Identifier() = default;
    [[nodiscard]] constexpr std::string getRawValue() const { return value; }
    [[nodiscard]] constexpr bool isCaseSensitive() const { return caseSensitive; }

    [[nodiscard]] constexpr std::string toString() const
    {
        std::string copy = value;
        if (!caseSensitive)
        {
            std::ranges::transform(
                copy.begin(),
                copy.end(),
                copy.begin(),
                [](const unsigned char character) -> unsigned char { return std::toupper(character); });
        }
        return copy;
    }

    friend bool operator==(const Identifier& lhs, const Identifier& rhs) { return lhs.toString() == rhs.toString(); }
    friend bool operator!=(const Identifier& lhs, const Identifier& rhs) { return !(lhs == rhs); }
};

class IdentifierList
{
    std::vector<Identifier> identifiers;

public:
    constexpr explicit IdentifierList() = default;
    constexpr explicit IdentifierList(std::ranges::input_range auto&& identifiers) : identifiers(identifiers) { }
    constexpr IdentifierList(const std::initializer_list<Identifier> identifiers) : identifiers(identifiers) { }
    constexpr IdentifierList(const Identifier& identifier) : identifiers(std::vector{identifier}) { }
    constexpr IdentifierList(const IdentifierList& other) = default;
    constexpr IdentifierList(IdentifierList&& other) noexcept = default;
    constexpr IdentifierList& operator=(const IdentifierList& other) = default;
    constexpr IdentifierList& operator=(IdentifierList&& other) noexcept = default;
    constexpr ~IdentifierList() = default;

    [[nodiscard]] constexpr decltype(identifiers.begin()) begin() { return identifiers.begin(); }
    [[nodiscard]] constexpr decltype(identifiers.end()) end() { return identifiers.end(); }

    [[nodiscard]] constexpr decltype(identifiers.cbegin()) begin() const { return identifiers.cbegin(); }
    [[nodiscard]] constexpr decltype(identifiers.cend()) end() const { return identifiers.cend(); }

    [[nodiscard]] constexpr IdentifierList copyReplaceLast(const std::initializer_list<Identifier> replacements) const
    {
        if (replacements.size() >= identifiers.size())
        {
            return IdentifierList{replacements};
        }
        auto newIdentifiers = std::vector{identifiers};

        const auto* iter = replacements.begin();
        for (auto i = identifiers.size() - replacements.size(); i < identifiers.size(); ++i)
        {
            newIdentifiers[i] = *iter;
            iter++;
        }
        return IdentifierList{std::move(newIdentifiers)};
    }

    [[nodiscard]] constexpr IdentifierList copyAppendLast(const std::ranges::input_range auto&& toAppend) const
    {
        auto newIdentifiers = std::vector{identifiers};
        newIdentifiers.reserve(identifiers.size() + toAppend.size());
        for (const auto& identifier : toAppend)
        {
            newIdentifiers.push_back(identifier);
        }
        return IdentifierList{std::move(newIdentifiers)};
    }

    [[nodiscard]] constexpr std::string toString() const
    {
        return identifiers | std::views::transform(&Identifier::toString) | std::views::join_with('.') | NES::ranges::to<std::string>();
    }

    // constexpr size_t size() const
    // {
    //     return identifiers.size();
    // }
    friend bool operator==(const IdentifierList& lhs, const IdentifierList& rhs) { return lhs.identifiers == rhs.identifiers; }
    friend bool operator!=(const IdentifierList& lhs, const IdentifierList& rhs) { return !(lhs == rhs); }

    IdentifierList operator+(const IdentifierList& other) const { return copyAppendLast(std::move(other)); }

    IdentifierList operator+(const Identifier& other) const { return copyAppendLast(std::initializer_list{other}); }
};

static_assert(std::ranges::input_range<std::initializer_list<Identifier>>);


static_assert(std::ranges::random_access_range<IdentifierList>);
static_assert(std::ranges::sized_range<IdentifierList>);
}

template <>
struct std::hash<NES::Identifier>
{
    std::size_t operator()(const NES::Identifier& arg) const noexcept { return std::hash<std::string>{}(arg.toString()); }
};

template <>
struct std::hash<NES::IdentifierList>
{
    //taken from https://stackoverflow.com/a/72073933 from SO user see,
    //based on https://stackoverflow.com/a/12996028 from SO user Thomas Mueller
    std::size_t operator()(const NES::IdentifierList& arg) const noexcept
    {
        std::size_t seed = std::ranges::size(arg);
        constexpr auto hasher = std::hash<NES::Identifier>{};
        for (const auto& identifier : arg)
        {
            auto hash = hasher(identifier);
            hash = ((hash >> 16) ^ hash) * 0x45d9f3b;
            hash = ((hash >> 16) ^ hash) * 0x45d9f3b;
            hash = (hash >> 16) ^ hash;
            seed ^= hash + 0x9e3779b9 + (seed << 6) + (seed >> 2);
        }
        return seed;
    }
};
