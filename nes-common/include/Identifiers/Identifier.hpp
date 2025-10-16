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
#include <ostream>
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
/// Reopresenting a SQL compliant identifier.
/// Automatically uppercases non-quoted identifiers but stores the original version so that we can emulate the behavior of non-SQL-compliant systems (e.g., Postgres) when interacting with them.
/// The template allows this class to be used with constexpr string, where we can only store a string_view.
/// For the most part, we should just use the "owning" version because it can't contain a dangling reference.
/// Should you use the non-owning version for non-constexpr strings, make sure that they don't get mutated while Identifiers wrapping them are alive.
/// The behaviour in that case is not well-defined and will sometimes produce correct results, sometimes exceptions and sometimes incorrect results.
template <bool Owning = true>
class IdentifierBase
{
    using valueType = std::conditional_t<Owning, std::string, std::string_view>;
    valueType value;
    bool caseSensitive;

    friend class IdentifierSerializationUtil;

    constexpr explicit IdentifierBase(const valueType value, const bool caseSensitive)
        : value(std::move(value)), caseSensitive(caseSensitive)
    {
    }

    explicit constexpr IdentifierBase(const valueType value) : value(value), caseSensitive(false) { }

    static constexpr bool isQuoted(std::string_view value)
    {
        PRECONDITION(value.find('.') == valueType::npos, "Invalid Identifier, cannot contain dot: {}", value);
        if (*std::ranges::begin(value) == '"')
        {
            PRECONDITION(*std::ranges::rbegin(value) == '"', "Invalid Identifier, must end with quote: {}", value);
            return true;
        }
        PRECONDITION(*std::ranges::begin(value) != '"', "Invalid Identifier, must not start but not end with quote: {}", value);
        return false;
    }

    static constexpr std::expected<bool, Exception> tryIsQuoted(std::string_view value)
    {
        if (value.find('.') != valueType::npos)
        {
            return std::unexpected{InvalidIdentifier("Cannot contain dot")};
        }
        if (*std::ranges::begin(value) == '"')
        {
            if (*std::ranges::rbegin(value) == '"')
            {
                return true;
            }
            return std::unexpected{InvalidIdentifier("{}", value)};
        }
        if (*std::ranges::rbegin(value) == '"')
        {
            return std::unexpected{InvalidIdentifier("{}", value)};
        }
        return false;
    }

public:
    // constexpr explicit IdentifierBase(std::string value) : value(std::move(value)) { }
    // constexpr explicit IdentifierBase(const std::string_view value) : value(value) { }
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
        caseSensitive = refIdentifier.isCaseSensitive();
    }

    //Should we keep the conversion operator or the conversion constructor?
    operator IdentifierBase<true>() const
    requires(!Owning)
    {
        return IdentifierBase<true>{std::string{value}, isCaseSensitive()};
    }

    /// Returns the original string this identifier was created with.
    /// DO NOT USE THIS METHOD FOR COMPARISON.
    /// Does not uppercase non-case sensitive identifiers as normally done.
    /// This method mainly exists for serialization purposes.
    [[nodiscard]] constexpr std::string_view getOriginalString() const { return value; }

    [[nodiscard]] constexpr bool isCaseSensitive() const
    {
        if constexpr (Owning)
        {
            return caseSensitive;
        }
        return isQuoted(value);
    }

    friend std::ostream& operator<<(std::ostream& os, const IdentifierBase& obj)
    {
        if (!obj.isCaseSensitive())
        {
            for (auto character :
                 std::views::transform(obj.value, [](const unsigned char character) -> unsigned char { return std::toupper(character); }))
            {
                os << character;
            }
            return os;
        }
        return os << std::string_view{obj.value}.substr(1, std::ranges::size(obj.value) - 2);
    }

    friend bool operator==(const IdentifierBase& lhs, const IdentifierBase& rhs)
    {
        std::stringstream lhsss;
        std::stringstream rhsss;
        lhsss << lhs;
        rhsss << rhs;
        return lhsss.str() == rhsss.str();
    }

    static constexpr IdentifierBase parse(valueType stringLike)
    {
        if constexpr (Owning)
        {
            auto caseSensitive = isQuoted(stringLike);
            return IdentifierBase{std::move(stringLike), caseSensitive};
        }
        else
        {
            /// Technically, it would not be needed to check here since we recalculate isQuoted evertime its needed,
            /// But we ensure that calling this method with an invalid input never succeeds
            auto caseSensitive = isQuoted(stringLike);
            return IdentifierBase{stringLike, caseSensitive};
        }
    }

    static constexpr std::expected<IdentifierBase, Exception> tryParse(valueType stringLike)
    {
        auto caseSensitive = tryIsQuoted(stringLike);
        if (caseSensitive.has_value())
        {
            if constexpr (Owning)
            {
                return IdentifierBase{std::move(stringLike), caseSensitive.value()};
            }
            else
            {
                return IdentifierBase{stringLike, caseSensitive.value()};
            }
        }
        return caseSensitive.error();
    }
};

using Identifier = IdentifierBase<true>;
}

template <bool Owning>
struct fmt::formatter<NES::IdentifierBase<Owning>> : ostream_formatter
{
};

template <bool owning>
struct std::hash<NES::IdentifierBase<owning>>
{
    std::size_t operator()(const NES::IdentifierBase<owning>& arg) const noexcept
    {
        return std::hash<std::string>{}(fmt::format("{}", arg));
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

namespace NES
{

class IdentifierList
{
    std::vector<Identifier> identifiers;

public:
    constexpr explicit IdentifierList(std::ranges::input_range auto&& identifiers)
        : identifiers(std::ranges::to<std::vector<Identifier>>(identifiers))
    {
        PRECONDITION(std::ranges::size(this->identifiers) > 0, "IdentifierList must not be empty");
    }

    constexpr IdentifierList(const std::initializer_list<Identifier> identifiers) : identifiers(identifiers)
    {
        PRECONDITION(std::ranges::size(this->identifiers) > 0, "IdentifierList must not be empty");
    }

    explicit constexpr IdentifierList(const Identifier& identifier) : identifiers(std::vector{std::move(identifier)}) { }

    constexpr IdentifierList() = delete;
    constexpr IdentifierList(const IdentifierList& other) = default;
    constexpr IdentifierList(IdentifierList&& other) noexcept = default;
    constexpr IdentifierList& operator=(const IdentifierList& other) = default;
    constexpr IdentifierList& operator=(IdentifierList&& other) noexcept = default;
    constexpr ~IdentifierList() = default;

    [[nodiscard]] constexpr decltype(identifiers.begin()) begin() { return identifiers.begin(); }

    [[nodiscard]] constexpr decltype(identifiers.end()) end() { return identifiers.end(); }

    [[nodiscard]] constexpr decltype(identifiers.cbegin()) begin() const { return identifiers.cbegin(); }

    [[nodiscard]] constexpr decltype(identifiers.cend()) end() const { return identifiers.cend(); }

    [[nodiscard]] size_t size() const { return identifiers.size(); }

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

    friend std::ostream& operator<<(std::ostream& os, const IdentifierList& obj)
    {
        return os
            << (obj.identifiers | std::views::transform([](const Identifier& identifier) { return fmt::format("{}", identifier); })
                | std::views::join_with('.') | std::ranges::to<std::string>());
    }

    friend bool operator==(const IdentifierList& lhs, const IdentifierList& rhs) { return lhs.identifiers == rhs.identifiers; }

    friend bool operator!=(const IdentifierList& lhs, const IdentifierList& rhs) { return !(lhs == rhs); }

    IdentifierList operator+(const IdentifierList& other) const { return copyAppendLast(std::move(other)); }

    IdentifierList operator+(const Identifier& other) const { return copyAppendLast(std::initializer_list<Identifier>{other}); }

    static std::optional<IdentifierList> parse(const std::string& name)
    {
        std::vector<Identifier> identifiers;
        std::stringstream stream(name);
        std::string item;
        while (std::getline(stream, item, '.'))
        {
            identifiers.push_back(Identifier::parse(item));
        }
        if (identifiers.size() == 0)
        {
            return std::nullopt;
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
            /// For some reason I didn't manage to get the type signatures right for ranges::all_of
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

template <>
struct fmt::formatter<NES::IdentifierList>
{
    constexpr auto parse(fmt::format_parse_context& ctx) { return ctx.begin(); }

    auto format(const NES::IdentifierList& identifiers, format_context& ctx) const
    {
        auto out = ctx.out();
        if (std::ranges::size(identifiers) >= 1)
        {
            out = fmt::format_to(out, "{}", *std::ranges::begin(identifiers));
        }
        for (const auto& identifier : std::span{std::ranges::begin(identifiers) + 1, std::ranges::end(identifiers)})
        {
            out = fmt::format_to(out, ".{}", identifier);
        }
        return out;
    }
};
