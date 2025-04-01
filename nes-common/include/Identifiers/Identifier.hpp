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
#include <format>
#include <functional>
#include <initializer_list>
#include <optional>
#include <ranges>
#include <span>
#include <sstream>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include <Util/Ranges.hpp>
#include <fmt/base.h>
#include <fmt/ostream.h>
#include <ErrorHandling.hpp>

namespace NES
{

//The template allows this class to be used with constexpr string, where we can only store a string_view.
//For most parts, we should just use the "owning" version because it can't contain a dangling reference.
template <bool Owning = true>
class IdentifierBase
{
    using valueType = std::conditional_t<Owning, std::string, std::string_view>;
    valueType value;
    bool caseSensitive{};

public:
    constexpr explicit IdentifierBase() = default;
    // constexpr explicit IdentifierBase(std::string value) : value(std::move(value)) { }
    // constexpr explicit IdentifierBase(const std::string_view value) : value(value) { }
    constexpr explicit IdentifierBase(const std::string_view value, const bool caseSensitive) : value(value), caseSensitive(caseSensitive)
    {
    }
    constexpr IdentifierBase(const IdentifierBase& other) = default;
    constexpr IdentifierBase(IdentifierBase&& other) noexcept = default;
    constexpr IdentifierBase& operator=(const IdentifierBase& other) = default;
    constexpr IdentifierBase& operator=(IdentifierBase&& other) noexcept = default;
    constexpr ~IdentifierBase() = default;


    // IdentifierBase(const IdentifierBase<false>& refIdentifier)
    // requires Owning
    //     : value(std::string{refIdentifier.value}), caseSensitive(refIdentifier.caseSensitive)
    // {
    // }

    IdentifierBase& operator=(const IdentifierBase<false>& refIdentifier)
    requires Owning
    {
        value = std::string{refIdentifier.value};
        caseSensitive = refIdentifier.caseSensitive;
    }


    //Should we keep the conversion operator or the conversion constructor?
    operator IdentifierBase<true>() const requires (!Owning)
    {
        return IdentifierBase<true>{std::string{value}, caseSensitive};
    }



    [[nodiscard]] constexpr std::string getRawValue() const { return std::string{value}; }
    [[nodiscard]] constexpr bool isCaseSensitive() const { return caseSensitive; }

    [[nodiscard]] constexpr std::string toString() const
    {
        auto copy = std::string{value};
        if (!caseSensitive)
        {
            std::ranges::transform(
                copy.begin(),
                copy.end(),
                copy.begin(),
                [](const unsigned char character) -> unsigned char { return std::toupper(character); });
            return copy;
        }
        return "\"" + copy + "\"";
    }

    friend constexpr bool operator==(const IdentifierBase& lhs, const IdentifierBase& rhs) { return lhs.toString() == rhs.toString(); }
    friend constexpr bool operator!=(const IdentifierBase& lhs, const IdentifierBase& rhs) { return !(lhs == rhs); }

    static constexpr IdentifierBase parse(std::string_view stringView)
    {
        if (*std::ranges::begin(stringView) == '"')
        {
            if (*(std::ranges::end(stringView) - 1) == '"')
            {
                return IdentifierBase{stringView.substr(1, std::ranges::size(stringView) - 2), true};
            }
            throw InvalidIdentifier("{}", stringView);
        }
        if (*(std::ranges::end(stringView) - 1) == '"')
        {
            throw InvalidIdentifier("{}", stringView);
        }
        return IdentifierBase{stringView, false};
    }

};

using Identifier = IdentifierBase<true>;
class IdentifierList
{
    std::vector<Identifier> identifiers;

public:
    constexpr explicit IdentifierList() = default;
    constexpr explicit IdentifierList(std::ranges::input_range auto&& identifiers)
        : identifiers(ranges::to<std::vector<Identifier>>(identifiers))
    {
    }
    constexpr IdentifierList(const std::initializer_list<Identifier> identifiers) : identifiers(identifiers) { }

    //The constructors from optionals can be removed in c++ 26 when optional implements the range concept
    constexpr IdentifierList(const std::optional<Identifier>& identifier)
    {
        if (identifier)
        {
            identifiers.push_back(*identifier);
        }
    }
    constexpr IdentifierList(const std::optional<IdentifierList>& identifier)
    {
        if (identifier)
        {
            identifiers = identifier->identifiers;
        }
    }
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

    [[nodiscard]] IdentifierList copyReplaceLast(const std::ranges::sized_range auto& replacements) const
    {
        if (std::ranges::size(replacements) >= identifiers.size())
        {
            return IdentifierList{replacements};
        }
        auto newIdentifiers = std::vector{identifiers};

        auto iter = std::ranges::begin(replacements);
        for (auto i = identifiers.size() - std::ranges::size(replacements); i < identifiers.size(); ++i)
        {
            newIdentifiers[i] = *iter;
            ++iter;
        }
        return IdentifierList{std::move(newIdentifiers)};
    }

    [[nodiscard]] IdentifierList copyAppendLast(const std::ranges::input_range auto&& toAppend) const
    {
        auto newIdentifiers = std::vector{identifiers};
        newIdentifiers.reserve(identifiers.size() + std::ranges::size(toAppend));
        for (const auto& identifier : toAppend)
        {
            newIdentifiers.push_back(identifier);
        }
        return IdentifierList{std::move(newIdentifiers)};
    }

    [[nodiscard]] std::string toString() const
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

    IdentifierList operator+(const Identifier& other) const { return copyAppendLast(std::initializer_list<Identifier>{other}); }

    static IdentifierList parse(const std::string& name)
    {
        std::vector<Identifier> identifiers;
        std::stringstream stream(name);
        std::string item;
        while (std::getline(stream, item, '.'))
        {
            identifiers.push_back(Identifier::parse(item));
        }
        return IdentifierList{identifiers};
    }
    struct SpanEquals
    {
        constexpr SpanEquals() = default;
        constexpr bool operator()(std::span<const Identifier> first, const std::span<const Identifier>& second) const
        {
            if (std::ranges::size(first) != std::ranges::size(second))
            {
                return false;
            }
            //For some reason I didn't manage to get the type signatures right for ranges::all_of
            for (auto zipped = std::views::zip(first, second); auto [first, second] : zipped)
            {
                if (first != second)
                {
                    return false;
                }
            }
            return true;
        }
    };
};

static_assert(std::ranges::input_range<std::initializer_list<Identifier>>);


static_assert(std::ranges::random_access_range<IdentifierList>);
static_assert(std::ranges::random_access_range<const IdentifierList>);
static_assert(std::ranges::sized_range<IdentifierList>);

IdentifierList zipIdentifierLists(const IdentifierList& firstIdList, const IdentifierList& secondIdList);
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

template <bool Owning>
struct std::hash<std::span<NES::IdentifierBase<Owning>>>
{
    //taken from https://stackoverflow.com/a/72073933 from SO user see,
    //based on https://stackoverflow.com/a/12996028 from SO user Thomas Mueller
    std::size_t operator()(const std::span<NES::IdentifierBase<Owning>>& arg) const noexcept
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


template <bool Owning>
struct std::hash<std::span<const NES::IdentifierBase<Owning>>>
{
    //taken from https://stackoverflow.com/a/72073933 from SO user see,
    //based on https://stackoverflow.com/a/12996028 from SO user Thomas Mueller
    std::size_t operator()(const std::span<const NES::IdentifierBase<Owning>>& arg) const noexcept
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

template <bool Owning>
struct std::formatter<NES::IdentifierBase<Owning>>
{
    constexpr auto parse(std::format_parse_context& ctx) { return ctx.begin(); }

    auto format(const NES::IdentifierBase<Owning>& identifier, std::format_context& ctx) const
    {
        return std::format_to(ctx.out(), "{}", identifier.toString());
    }
};

template <>
struct std::formatter<NES::IdentifierList>
{
    constexpr auto parse(std::format_parse_context& ctx) { return ctx.begin(); }

    auto format(const NES::IdentifierList& identifiers, std::format_context& ctx) const
    {
        auto out = ctx.out();
        if (std::ranges::size(identifiers) >= 1)
        {
            out = std::format_to(out, "{}", std::ranges::begin(identifiers)->toString());
        }
        for (const auto& identifier : std::span{std::ranges::begin(identifiers) + 1, std::ranges::end(identifiers)})
        {
            out = std::format_to(out, ".{}", identifier.toString());
        }
        return out;
    }
};

template <bool Owning>
struct fmt::formatter<NES::IdentifierBase<Owning>>
{
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }

    auto format(const NES::IdentifierBase<Owning>& identifier, format_context& ctx) const
    {
        return fmt::format_to(ctx.out(), "{}", identifier.toString());
    }
};
template <>
struct fmt::formatter<NES::IdentifierList>
{
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }

    auto format(const NES::IdentifierList& identifiers, format_context& ctx) const
    {
        auto out = ctx.out();
        if (std::ranges::size(identifiers) >= 1)
        {
            out = fmt::format_to(out, "{}", std::ranges::begin(identifiers)->toString());
        }
        for (const auto& identifier : std::span{std::ranges::begin(identifiers) + 1, std::ranges::end(identifiers)})
        {
            out = fmt::format_to(out, ".{}", identifier.toString());
        }
        return out;
    }
};
