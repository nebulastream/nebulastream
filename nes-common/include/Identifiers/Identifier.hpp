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

    constexpr IdentifierBase(const std::string_view value) : value(value), caseSensitive(false) { }

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
    operator IdentifierBase<true>() const
    requires(!Owning)
    {
        return IdentifierBase<true>{std::string{value}, caseSensitive};
    }

    [[nodiscard]] constexpr std::string getRawValue() const { return std::string{value}; }

    [[nodiscard]] constexpr bool isCaseSensitive() const { return caseSensitive; }

    friend std::ostream& operator<<(std::ostream& os, const IdentifierBase& obj)
    {
        auto copy = std::string{obj.value};
        if (!obj.caseSensitive)
        {
            std::ranges::transform(
                copy.begin(),
                copy.end(),
                copy.begin(),
                [](const unsigned char character) -> unsigned char { return std::toupper(character); });
            return os << copy;
        }
        return os << "\"" + copy + "\"";
    }

    friend bool operator==(const IdentifierBase& lhs, const IdentifierBase& rhs)
    {
        std::stringstream lhsss;
        std::stringstream rhsss;
        lhsss << lhs;
        rhsss << rhs;
        return lhsss.str() == rhsss.str();
    }

    friend bool operator!=(const IdentifierBase& lhs, const IdentifierBase& rhs) { return !(lhs == rhs); }

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

template <size_t maxSize, bool owning>
class IdentifierListBase;

///A list of identifiers that contains at least one element
template <size_t maxSize, bool owning>
requires(maxSize == std::dynamic_extent && owning)
class IdentifierListBase<maxSize, owning>
{
    std::vector<Identifier> identifiers;

public:
    constexpr explicit IdentifierListBase(std::ranges::input_range auto&& identifiers)
        : identifiers(std::ranges::to<std::vector<Identifier>>(identifiers))
    {
        PRECONDITION(std::ranges::size(identifiers) > 0, "IdentifierList must not be empty");
    }

    constexpr IdentifierListBase(const std::initializer_list<Identifier> identifiers) : identifiers(identifiers)
    {
        PRECONDITION(std::ranges::size(identifiers) > 0, "IdentifierList must not be empty");
    }

    explicit constexpr IdentifierListBase(const Identifier& identifier) : identifiers(std::vector{identifier}) { }

    constexpr IdentifierListBase(const IdentifierListBase& other) = default;
    constexpr IdentifierListBase(IdentifierListBase&& other) noexcept = default;
    constexpr IdentifierListBase& operator=(const IdentifierListBase& other) = default;
    constexpr IdentifierListBase& operator=(IdentifierListBase&& other) noexcept = default;
    constexpr ~IdentifierListBase() = default;

    [[nodiscard]] constexpr decltype(identifiers.begin()) begin() { return identifiers.begin(); }

    [[nodiscard]] constexpr decltype(identifiers.end()) end() { return identifiers.end(); }

    [[nodiscard]] constexpr decltype(identifiers.cbegin()) begin() const { return identifiers.cbegin(); }

    [[nodiscard]] constexpr decltype(identifiers.cend()) end() const { return identifiers.cend(); }

    [[nodiscard]] size_t size() const { return identifiers.size(); }

    [[nodiscard]] IdentifierListBase copyAppendLast(const std::ranges::input_range auto&& toAppend) const
    {
        auto newIdentifiers = std::vector{identifiers};
        newIdentifiers.reserve(identifiers.size() + std::ranges::size(toAppend));
        for (const auto& identifier : toAppend)
        {
            newIdentifiers.push_back(identifier);
        }
        return IdentifierListBase{std::move(newIdentifiers)};
    }

    friend std::ostream& operator<<(std::ostream& os, const IdentifierListBase& obj)
    {
        return os
            << (obj.identifiers | std::views::transform([](const Identifier& identifier) { return fmt::format("{}", identifier); })
                | std::views::join_with('.') | std::ranges::to<std::string>());
    }

    friend bool operator==(const IdentifierListBase& lhs, const IdentifierListBase& rhs) { return lhs.identifiers == rhs.identifiers; }

    friend bool operator!=(const IdentifierListBase& lhs, const IdentifierListBase& rhs) { return !(lhs == rhs); }

    IdentifierListBase operator+(const IdentifierListBase& other) const { return copyAppendLast(std::move(other)); }

    IdentifierListBase operator+(const Identifier& other) const { return copyAppendLast(std::initializer_list<Identifier>{other}); }

    static std::optional<IdentifierListBase> parse(const std::string& name)
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
        return IdentifierListBase{identifiers};
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

template <size_t maxSize, bool owning>
requires(maxSize != std::dynamic_extent && maxSize > 0)
class IdentifierListBase<maxSize, owning>
{
    std::array<IdentifierBase<owning>, maxSize> identifiers;

public:
    constexpr explicit IdentifierListBase(std::array<IdentifierBase<owning>, maxSize> identifiers) : identifiers(std::move(identifiers)) { }

    constexpr IdentifierListBase(const std::initializer_list<Identifier> identifiers) : identifiers(identifiers)
    {
        static_assert(
            std::ranges::size(identifiers) == maxSize,
            "Trying to initialize bounded IdentifierList with the incorrect amount of identifiers");
    }

    template <size_t n>
    requires(n == maxSize && n == 1)
    constexpr IdentifierListBase(const Identifier& identifier) : identifiers{identifier}
    {
    }

    constexpr IdentifierListBase(const IdentifierListBase& other) = default;
    constexpr IdentifierListBase(IdentifierListBase&& other) noexcept = default;
    constexpr IdentifierListBase& operator=(const IdentifierListBase& other) = default;
    constexpr IdentifierListBase& operator=(IdentifierListBase&& other) noexcept = default;
    constexpr ~IdentifierListBase() = default;

    [[nodiscard]] constexpr decltype(identifiers.begin()) begin() { return identifiers.begin(); }

    [[nodiscard]] constexpr decltype(identifiers.end()) end() { return identifiers.end(); }

    [[nodiscard]] constexpr decltype(identifiers.cbegin()) begin() const { return identifiers.cbegin(); }

    [[nodiscard]] constexpr decltype(identifiers.cend()) end() const { return identifiers.cend(); }

    [[nodiscard]] constexpr size_t size() { return maxSize; }

    friend std::ostream& operator<<(std::ostream& os, const IdentifierListBase& obj)
    {
        return os
            << (obj.identifiers | std::views::transform([](const Identifier& identifier) { return fmt::format("{}", identifier); })
                | std::views::join_with('.') | std::ranges::to<std::string>());
    }

    friend bool operator==(const IdentifierListBase& lhs, const IdentifierListBase& rhs) { return lhs.identifiers == rhs.identifiers; }

    friend bool operator!=(const IdentifierListBase& lhs, const IdentifierListBase& rhs) { return !(lhs == rhs); }

    template <size_t size>
    static std::optional<IdentifierListBase> parse(const std::string& name)
    {
        std::array<Identifier, size> identifiers;
        std::stringstream stream(name);
        std::string item;
        size_t index = 0;
        while (std::getline(stream, item, '.'))
        {
            if (index >= size)
            {
                return std::nullopt;
            }
            identifiers[index] = Identifier::parse(item);
            index++;
        }

        if (index < size)
        {
            return std::nullopt;
        }
        return IdentifierListBase{identifiers};
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

using IdentifierList = IdentifierListBase<std::dynamic_extent, true>;
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
