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
#include <fmt/ranges.h>
#include <ErrorHandling.hpp>

namespace NES
{

template <size_t Extent>
requires(Extent >= 1)
class IdentifierListBase;

/// Reopresenting a SQL compliant identifier.
/// Automatically uppercases non-quoted identifiers but stores the original version so that we can emulate the behavior of non-SQL-compliant systems (e.g., Postgres) when interacting with them.
/// The template allows this class to be used with constexpr string, where we can only store a string_view.
/// For the most part, we should just use the "owning" version because it can't contain a dangling reference.
/// Should you use the non-owning version for non-constexpr strings, make sure that they don't get mutated while Identifiers wrapping them are alive.
/// The behaviour in that case is not well-defined and will sometimes produce correct results, sometimes exceptions and sometimes incorrect results.
template <size_t Extent>
class IdentifierBase
{
    using ValueType = std::conditional_t<Extent == std::dynamic_extent, std::string, std::array<char, Extent>>;
    ValueType value;
    bool caseSensitive;

    friend class IdentifierSerializationUtil;

    template <size_t OtherExtent>
    friend class IdentifierBase;

    constexpr explicit IdentifierBase(const ValueType value, const bool caseSensitive)
    requires(Extent == std::dynamic_extent)
        : value(std::move(value)), caseSensitive(caseSensitive)
    {
    }

    constexpr explicit IdentifierBase(const ValueType value, const bool caseSensitive)
    requires(Extent != std::dynamic_extent)
        : value(value), caseSensitive(caseSensitive)
    {
    }

    constexpr IdentifierBase() = default;

    static constexpr bool isQuoted(std::string_view value)
    {
        PRECONDITION(value.find('.') == std::string_view::npos, "Invalid Identifier, cannot contain dot: {}", value);
        if (*std::ranges::begin(value) == '"')
        {
            PRECONDITION(*std::ranges::rbegin(value) == '"', "Invalid Identifier, must end with quote: {}", value);
            return true;
        }
        PRECONDITION(
            *std::ranges::rbegin(value) != '"', "Invalid Identifier, cannot end with quote if it doesn't start with one: {}", value);
        return false;
    }

    static constexpr std::expected<bool, Exception> tryIsQuoted(std::string_view value)
    {
        if (value.find('.') != std::string_view::npos)
        {
            return std::unexpected{InvalidIdentifier("Cannot contain dot")};
        }
        if (*std::ranges::begin(value) == '"')
        {
            if (*std::ranges::rbegin(value) == '"')
            {
                return true;
            }
            return std::unexpected{InvalidIdentifier("Must also end with quote: {}", value)};
        }
        if (*std::ranges::rbegin(value) == '"')
        {
            return std::unexpected{InvalidIdentifier("Cannot end with quote if it doesn't start with one: {}", value)};
        }
        return false;
    }

public:
    constexpr IdentifierBase(const IdentifierBase& other) = default;
    constexpr IdentifierBase(IdentifierBase&& other) noexcept = default;
    constexpr IdentifierBase& operator=(const IdentifierBase& other) = default;
    constexpr IdentifierBase& operator=(IdentifierBase&& other) noexcept = default;
    constexpr ~IdentifierBase() = default;

    template <size_t OtherExtent>
    IdentifierBase& operator=(const IdentifierBase<OtherExtent>& refIdentifier)
    requires(OtherExtent == std::dynamic_extent)
    {
        value = std::string{refIdentifier.value.begin(), refIdentifier.value.end()};
        caseSensitive = refIdentifier.isCaseSensitive();
    }

    //Should we keep the conversion operator or the conversion constructor?
    operator IdentifierBase<std::dynamic_extent>() const
    requires(Extent != std::dynamic_extent)
    {
        return IdentifierBase<std::dynamic_extent>{std::string{value.begin(), value.end()}, isCaseSensitive()};
    }

    operator IdentifierListBase<1>() const;

    operator IdentifierListBase<std::dynamic_extent>() const;

    /// Returns the original string this identifier was created with.
    /// DO NOT USE THIS METHOD FOR COMPARISON.
    /// Does not uppercase non-case sensitive identifiers as normally done.
    /// This method mainly exists for serialization purposes.
    [[nodiscard]] constexpr std::string_view getOriginalString() const { return std::string_view{value.begin(), value.end()}; }

    [[nodiscard]] constexpr bool isCaseSensitive() const { return caseSensitive; }

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

    [[nodiscard]] std::string asCanonicalString() const { return fmt::format("{}", *this); }

    template <size_t Rhs_Extent>
    friend bool operator==(const IdentifierBase& lhs, const IdentifierBase<Rhs_Extent>& rhs)
    // requires (Extent == std::dynamic_extent)
    {
        std::stringstream lhsss;
        std::stringstream rhsss;
        lhsss << lhs;
        rhsss << rhs;
        return lhsss.str() == rhsss.str();
    }

    /// We attach these static parse methods to the template specialization with dynamic extent so that it is available under Identifier::parse
    /// But don't start generating unnecessary specializations by accident.

    static constexpr IdentifierBase<std::dynamic_extent> parse(std::string value)
    requires(Extent == std::dynamic_extent)
    {
        PRECONDITION(std::ranges::size(value) != 0, "Invalid identifier, cannot be empty");
        auto caseSensitive = isQuoted(value);
        return IdentifierBase{std::move(value), caseSensitive};
    }

    template <size_t ReturnedExtent>
    static constexpr IdentifierBase<ReturnedExtent - 1> parse(const char (&value)[ReturnedExtent])
    requires(Extent == std::dynamic_extent && ReturnedExtent > 1)
    {
        PRECONDITION(value[ReturnedExtent - 1] == '\0', "Invalid Identifier, must end with null terminator: {}", value);
        IdentifierBase<ReturnedExtent - 1> identifier{};
        /// While string view can deal with a null byte, isQuoted expects the last character to be a quote if the first one is one
        identifier.caseSensitive = isQuoted(std::string_view{value, ReturnedExtent - 1});
        std::copy_n(value, ReturnedExtent - 1, identifier.value.data());
        return identifier;
    }

    static constexpr std::expected<IdentifierBase, Exception> tryParse(std::string value)
    requires(Extent == std::dynamic_extent)
    {
        if (std::ranges::size(value) == 0)
        {
            return std::unexpected{InvalidIdentifier("Cannot be empty")};
        }
        auto caseSensitive = tryIsQuoted(value);
        if (caseSensitive.has_value())
        {
            return IdentifierBase{std::move(value), caseSensitive.value()};
        }
        return std::unexpected{caseSensitive.error()};
    }

    template <size_t ReturnedExtent>
    static constexpr std::expected<IdentifierBase<ReturnedExtent - 1>, Exception> tryParse(const char (&value)[ReturnedExtent])
    requires(Extent == std::dynamic_extent && ReturnedExtent > 1)
    {
        auto caseSensitive = tryIsQuoted(value);
        if (caseSensitive.has_value())
        {
            IdentifierBase<ReturnedExtent - 1> identifier{};
            identifier.caseSensitive = caseSensitive.value();
            std::copy_n(value, ReturnedExtent - 1, identifier.value.data());
            return identifier;
        }
        return std::unexpected{caseSensitive.error()};
    }
};

using Identifier = IdentifierBase<std::dynamic_extent>;

}

template <size_t Extent>
struct fmt::formatter<NES::IdentifierBase<Extent>> : ostream_formatter
{
};

template <size_t Extent>
struct std::hash<NES::IdentifierBase<Extent>>
{
    std::size_t operator()(const NES::IdentifierBase<Extent>& arg) const noexcept
    {
        return std::hash<std::string>{}(fmt::format("{}", arg));
    }
};

template <size_t Extent, size_t SpanExtent>
struct std::hash<std::span<NES::IdentifierBase<Extent>, SpanExtent>>
{
    //taken from https://stackoverflow.com/a/72073933 from SO user see,
    //based on https://stackoverflow.com/a/12996028 from SO user Thomas Mueller
    std::size_t operator()(const std::span<NES::IdentifierBase<Extent>, SpanExtent>& arg) const noexcept
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

template <size_t Extent, size_t SpanExtent>
struct std::hash<std::span<const NES::IdentifierBase<Extent>, SpanExtent>>
{
    //taken from https://stackoverflow.com/a/72073933 from SO user see,
    //based on https://stackoverflow.com/a/12996028 from SO user Thomas Mueller
    std::size_t operator()(const std::span<const NES::IdentifierBase<Extent>, SpanExtent>& arg) const noexcept
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

template <size_t Extent>
requires(Extent >= 1)
class IdentifierListBase
{
    using ContainerType = std::conditional_t<Extent == std::dynamic_extent, std::vector<Identifier>, std::array<Identifier, Extent>>;
    ContainerType identifiers;

    template <size_t OtherExtent>
    requires(OtherExtent >= 1)
    friend class IdentifierListBase;

public:
    constexpr explicit IdentifierListBase(ContainerType identifiers) : identifiers(std::move(identifiers))
    {
        PRECONDITION(std::ranges::size(this->identifiers) > 0, "IdentifierList must not be empty");
    }

    constexpr explicit IdentifierListBase(std::span<const Identifier, Extent> idSpan)
    {
        if constexpr (Extent == std::dynamic_extent)
        {
            for (auto& identifier : idSpan)
            {
                identifiers.emplace_back(identifier);
            }
        }
        else
        {
            for (auto& [idx, identifier] : views::enumerate(idSpan))
            {
                identifiers[idx] = identifier;
            }
        }
    }

    template <std::ranges::input_range T>
    requires(
        std::same_as<std::remove_cv_t<std::ranges::range_value_t<T>>, Identifier> && !std::same_as<T, ContainerType>
        && Extent == std::dynamic_extent)
    explicit constexpr IdentifierListBase(const T& identifiers) : identifiers(identifiers | std::ranges::to<std::vector>())
    {
        PRECONDITION(std::ranges::size(this->identifiers) > 0, "IdentifierList must not be empty");
    }

    // constexpr IdentifierListBase(const std::initializer_list<Identifier>& identifiers) : identifiers(identifiers)
    // {
    //     PRECONDITION(std::ranges::size(this->identifiers) > 0, "IdentifierList must not be empty");
    // }

    // constexpr IdentifierListBase(Identifier identifier) : identifiers(ContainerType{std::move(identifier)}) { }


    constexpr IdentifierListBase() = delete;
    constexpr IdentifierListBase(const IdentifierListBase& other) = default;
    constexpr IdentifierListBase(IdentifierListBase&& other) noexcept = default;
    constexpr IdentifierListBase& operator=(const IdentifierListBase& other) = default;
    constexpr IdentifierListBase& operator=(IdentifierListBase&& other) noexcept = default;
    constexpr ~IdentifierListBase() = default;

    constexpr operator const Identifier&() const
    requires(Extent == 1)
    {
        return identifiers[0];
    }

    operator IdentifierListBase<std::dynamic_extent>() const
    requires(Extent != std::dynamic_extent)
    {
        return IdentifierListBase<std::dynamic_extent>(identifiers | std::ranges::to<std::vector>());
    }

    [[nodiscard]] constexpr decltype(identifiers.begin()) begin() { return identifiers.begin(); }

    [[nodiscard]] constexpr decltype(identifiers.end()) end() { return identifiers.end(); }

    [[nodiscard]] constexpr decltype(identifiers.cbegin()) begin() const { return identifiers.cbegin(); }

    [[nodiscard]] constexpr decltype(identifiers.cend()) end() const { return identifiers.cend(); }

    [[nodiscard]] constexpr size_t size() const { return identifiers.size(); }

    [[nodiscard]] IdentifierListBase<std::dynamic_extent> copyAppendLast(const std::ranges::input_range auto&& toAppend) const
    {
        auto newIdentifiers = identifiers | std::ranges::to<std::vector<Identifier>>();
        newIdentifiers.reserve(std::ranges::size(identifiers) + std::ranges::size(toAppend));
        for (const auto& identifier : toAppend)
        {
            newIdentifiers.push_back(identifier);
        }
        return IdentifierListBase<std::dynamic_extent>{std::move(newIdentifiers)};
    }

    friend std::ostream& operator<<(std::ostream& os, const IdentifierListBase& obj)
    {
        return os << fmt::format(
                   "{}",
                   fmt::join(
                       obj.identifiers | std::views::transform([](const Identifier& identifier) { return fmt::format("{}", identifier); }),
                       "."));
    }

    template <size_t Rhs_Extent>
    friend bool operator==(const IdentifierListBase& lhs, const IdentifierListBase<Rhs_Extent>& rhs)
    {
        if (std::ranges::size(lhs.identifiers) != std::ranges::size(rhs.identifiers))
        {
            return false;
        }
        for (auto zipped = std::views::zip(lhs.identifiers, rhs.identifiers); auto [lhs, rhs] : zipped)
        {
            if (lhs != rhs)
            {
                return false;
            }
        }
        return true;
    }

    IdentifierListBase<std::dynamic_extent> operator+(const std::ranges::input_range auto& other) const
    {
        return copyAppendLast(std::move(other));
    }

    IdentifierListBase<std::dynamic_extent> operator+(const Identifier& other) const
    {
        auto newIdentifiers = identifiers | std::ranges::to<std::vector<Identifier>>();
        newIdentifiers.push_back(other);
        return IdentifierListBase<std::dynamic_extent>{std::move(newIdentifiers)};
    }

    static std::expected<IdentifierListBase, Exception> tryCreate(std::vector<Identifier> identifiers)
    requires(Extent == std::dynamic_extent)
    {
        if (std::ranges::empty(identifiers))
        {
            return std::unexpected{InvalidIdentifier("Cannot be empty")};
        }
        return IdentifierListBase<std::dynamic_extent>{std::move(identifiers)};
    }

    template <size_t CreatedExtent>
    static IdentifierListBase<CreatedExtent> create(std::array<Identifier, CreatedExtent> identifiers)
    requires(CreatedExtent >= 1)
    {
        return IdentifierListBase<CreatedExtent>{std::move(identifiers)};
    }

    template <typename... T>
    static IdentifierListBase<sizeof...(T)> create(const T&... identifiers)
    requires(sizeof...(T) >= 1 && Extent == std::dynamic_extent)
    {
        return IdentifierListBase<sizeof...(T)>{std::array{Identifier(identifiers)...}};
    }

    static std::expected<IdentifierListBase, Exception> tryParse(std::string_view name)
    requires(Extent == std::dynamic_extent)
    {
        std::vector<Identifier> identifiers;
        for (const auto& item : name | std::views::split('.'))
        {
            auto identifierExpected = Identifier::tryParse(std::string{item.begin(), item.end()});
            if (identifierExpected.has_value())
            {
                identifiers.push_back(std::move(identifierExpected.value()));
            }
            else
            {
                return std::unexpected{std::move(identifierExpected.error())};
            }
        }
        return tryCreate(std::move(identifiers));
    }

    template <size_t SpanExtent = std::dynamic_extent>
    struct SpanEquals
    {
        constexpr SpanEquals() = default;

        constexpr bool
        operator()(std::span<const Identifier, SpanExtent> first, const std::span<const Identifier, SpanExtent>& second) const
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

using IdentifierList = IdentifierListBase<std::dynamic_extent>;
static_assert(std::ranges::input_range<std::initializer_list<Identifier>>);

static_assert(std::ranges::input_range<IdentifierList>);
static_assert(std::ranges::random_access_range<IdentifierList>);
static_assert(std::ranges::random_access_range<const IdentifierList>);
static_assert(std::ranges::sized_range<IdentifierList>);

template <size_t Extent>
IdentifierBase<Extent>::operator IdentifierListBase<1>() const
{
    return IdentifierListBase<1>{std::array{Identifier{*this}}};
}

template <size_t Extent>
IdentifierBase<Extent>::operator IdentifierListBase<std::dynamic_extent>() const
{
    return IdentifierList{std::vector{Identifier{*this}}};
}
}

template <size_t Extent>
struct std::hash<NES::IdentifierListBase<Extent>>
{
    ///taken from https://stackoverflow.com/a/72073933 from SO user see,
    ///based on https://stackoverflow.com/a/12996028 from SO user Thomas Mueller
    ///not using folly hash to avoid drawing it in as a dependency for every file that uses identifiers
    std::size_t operator()(const NES::IdentifierListBase<Extent>& arg) const noexcept
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

FMT_OSTREAM(NES::IdentifierList);