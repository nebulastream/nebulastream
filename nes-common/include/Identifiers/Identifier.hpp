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
#include <array>
#include <cctype>
#include <concepts>
#include <cstddef>
#include <expected>
#include <functional>
#include <initializer_list>
#include <iterator>
#include <ostream>
#include <ranges>
#include <span>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include <Util/Reflection.hpp>
#include <fmt/base.h>
#include <fmt/format.h>
#include <fmt/ostream.h>
#include <fmt/ranges.h>
#include <folly/hash/Hash.h>
#include <ErrorHandling.hpp>

namespace NES
{

template <size_t Extent>
requires(Extent >= 1)
class QualifiedIdentifierBase;

/// Representing a SQL compliant identifier.
/// Automatically uppercases non-quoted identifiers but stores the original version so that we can emulate the behavior of non-SQL-compliant systems (e.g., Postgres) when interacting with them.
class Identifier
{
    std::string value;
    bool caseSensitive{};

    friend class IdentifierSerializationUtil;

    template <size_t IdListExtent>
    requires(IdListExtent >= 1)
    friend class QualifiedIdentifierBase;

    friend struct Unreflector<Identifier>;

    explicit Identifier(std::string value, const bool caseSensitive) : value(std::move(value)), caseSensitive(caseSensitive) { }

    Identifier() = default;

    static bool isQuoted(std::string_view value)
    {
        PRECONDITION(value.find('.') == std::string_view::npos, "Invalid identifier, cannot contain dot: {}", value);
        if (value.front() == '"')
        {
            PRECONDITION(value.back() == '"', "Invalid identifier, must end with quote: {}", value);
            return true;
        }
        PRECONDITION(value.back() != '"', "Invalid identifier, cannot end with quote if it doesn't start with one: {}", value);
        return false;
    }

    static std::expected<bool, Exception> tryIsQuoted(std::string_view value)
    {
        if (value.find('.') != std::string_view::npos)
        {
            return std::unexpected{InvalidIdentifier("Cannot contain dot")};
        }
        if (value.front() == '"')
        {
            if (value.back() == '"')
            {
                return true;
            }
            return std::unexpected{InvalidIdentifier("Must also end with quote: {}", value)};
        }
        if (value.back() == '"')
        {
            return std::unexpected{InvalidIdentifier("Cannot end with quote if it doesn't start with one: {}", value)};
        }
        return false;
    }

public:
    Identifier(const Identifier& other) = default;
    Identifier(Identifier&& other) noexcept = default;
    Identifier& operator=(const Identifier& other) = default;
    Identifier& operator=(Identifier&& other) noexcept = default;
    ~Identifier() = default;

    /// NOLINTNEXTLINE(google-explicit-constructor)
    operator QualifiedIdentifierBase<1>() const;

    /// NOLINTNEXTLINE(google-explicit-constructor)
    operator QualifiedIdentifierBase<std::dynamic_extent>() const;

    /// Returns the original string this identifier was created with.
    /// DO NOT USE THIS METHOD FOR COMPARISON.
    /// Does not uppercase non-case sensitive identifiers as normally done.
    /// This method only exists to enable compatibility with systems that follow a different convention.
    [[nodiscard]] std::string_view getOriginalString() const { return value; }

    [[nodiscard]] bool isCaseSensitive() const { return caseSensitive; }

    friend std::ostream& operator<<(std::ostream& os, const Identifier& obj);

    [[nodiscard]] std::string asCanonicalString() const;

    friend bool operator==(const Identifier& lhs, const Identifier& rhs) { return lhs.asCanonicalString() == rhs.asCanonicalString(); }

    static Identifier parse(std::string value)
    {
        PRECONDITION(!value.empty(), "Invalid identifier, cannot be empty");
        const auto caseSensitive = isQuoted(value);
        return Identifier{std::move(value), caseSensitive};
    }

    static std::expected<Identifier, Exception> tryParse(std::string value)
    {
        if (value.empty())
        {
            return std::unexpected{InvalidIdentifier("Cannot be empty")};
        }
        auto caseSensitive = tryIsQuoted(value);
        if (!caseSensitive.has_value())
        {
            return std::unexpected{caseSensitive.error()};
        }
        return Identifier{std::move(value), caseSensitive.value()};
    }
};

template <>
struct Reflector<Identifier>
{
    Reflected operator()(const Identifier& identifier) const
    {
        return reflect(std::pair{identifier.getOriginalString(), identifier.isCaseSensitive()});
    }
};

template <>
struct Unreflector<Identifier>
{
    Identifier operator()(const Reflected& reflectable, const ReflectionContext& context) const
    {
        const auto [val, caseSensitive] = context.unreflect<std::pair<std::string, bool>>(reflectable);
        return Identifier{val, caseSensitive};
    }
};
}

template <>
struct fmt::formatter<NES::Identifier>
{
    static constexpr auto parse(const format_parse_context& ctx) -> decltype(ctx.begin()) { return ctx.begin(); }

    [[nodiscard]] static auto format(const NES::Identifier& obj, format_context& ctx) -> decltype(ctx.out())
    {
        const auto str = obj.getOriginalString();
        if (!obj.isCaseSensitive())
        {
            std::string upper;
            upper.reserve(str.size());
            std::ranges::transform(
                str, std::back_inserter(upper), [](char c) { return static_cast<char>(std::toupper(static_cast<unsigned char>(c))); });
            return fmt::format_to(ctx.out(), "{}", upper);
        }
        PRECONDITION(str.size() >= 2, "Invalid identifier, cannot be empty");
        return fmt::format_to(ctx.out(), "{}", str.substr(1, str.size() - 2));
    }
};

static_assert(fmt::formattable<NES::Identifier>);

namespace NES
{
inline std::string Identifier::asCanonicalString() const
{
    return fmt::format("{}", *this);
}

inline std::ostream& operator<<(std::ostream& os, const Identifier& obj)
{
    return os << fmt::format("{}", obj);
}
}

/// NOLINTBEGIN(cert-dcl58-cpp)
template <>
struct std::hash<NES::Identifier>
{
    std::size_t operator()(const NES::Identifier& arg) const noexcept { return std::hash<std::string>{}(arg.asCanonicalString()); }
};

template <size_t SpanExtent>
struct std::hash<std::span<NES::Identifier, SpanExtent>>
{
    std::size_t operator()(const std::span<NES::Identifier, SpanExtent>& arg) const noexcept
    {
        return folly::hash::hash_range(std::ranges::begin(arg), std::ranges::end(arg));
    }
};

template <size_t SpanExtent>
struct std::hash<std::span<const NES::Identifier, SpanExtent>>
{
    std::size_t operator()(const std::span<const NES::Identifier, SpanExtent>& arg) const noexcept
    {
        return folly::hash::hash_range(std::ranges::begin(arg), std::ranges::end(arg));
    }
};

/// NOLINTEND(cert-dcl58-cpp)

namespace NES
{

namespace detail
{
template <size_t Extent, size_t... Is>
requires(Extent >= sizeof...(Is))
std::array<Identifier, sizeof...(Is)> toArray(std::span<const Identifier, Extent> identifiers, std::index_sequence<Is...>)
{
    return {identifiers[Is]...};
}
}

template <size_t Extent>
requires(Extent >= 1)
class QualifiedIdentifierBase
{
    using ContainerType = std::conditional_t<Extent == std::dynamic_extent, std::vector<Identifier>, std::array<Identifier, Extent>>;
    ContainerType identifiers;

    template <size_t OtherExtent>
    requires(OtherExtent >= 1)
    friend class QualifiedIdentifierBase;
    friend struct Reflector<QualifiedIdentifierBase>;
    friend struct Unreflector<QualifiedIdentifierBase>;

public:
    constexpr explicit QualifiedIdentifierBase(ContainerType identifiers) : identifiers(std::move(identifiers))
    {
        PRECONDITION(std::ranges::size(this->identifiers) > 0, "QualifiedIdentifier must not be empty");
    }

    constexpr explicit QualifiedIdentifierBase(std::span<const Identifier, Extent> idSpan)
        : identifiers(
              [&idSpan]()
              {
                  if constexpr (Extent == std::dynamic_extent)
                  {
                      std::vector<Identifier> identifiers;
                      for (const auto& identifier : idSpan)
                      {
                          identifiers.emplace_back(identifier);
                      }
                      return identifiers;
                  }
                  else
                  {
                      return detail::toArray(idSpan, std::make_index_sequence<Extent>{});
                  }
              }())
    {
    }

    template <std::ranges::input_range T>
    requires(
        std::same_as<std::remove_cv_t<std::ranges::range_value_t<T>>, Identifier> && !std::same_as<T, ContainerType>
        && Extent == std::dynamic_extent)
    explicit constexpr QualifiedIdentifierBase(const T& identifiers) : identifiers(identifiers | std::ranges::to<std::vector>())
    {
        PRECONDITION(std::ranges::size(this->identifiers) > 0, "QualifiedIdentifier must not be empty");
    }

    constexpr QualifiedIdentifierBase() = delete;
    constexpr QualifiedIdentifierBase(const QualifiedIdentifierBase& other) = default;
    constexpr QualifiedIdentifierBase(QualifiedIdentifierBase&& other) noexcept = default;
    constexpr QualifiedIdentifierBase& operator=(const QualifiedIdentifierBase& other) = default;
    constexpr QualifiedIdentifierBase& operator=(QualifiedIdentifierBase&& other) noexcept = default;
    constexpr ~QualifiedIdentifierBase() = default;

    /// NOLINTNEXTLINE(google-explicit-constructor)
    constexpr operator const Identifier&() const
    requires(Extent == 1)
    {
        return identifiers[0];
    }

    /// NOLINTNEXTLINE(google-explicit-constructor)
    operator QualifiedIdentifierBase<std::dynamic_extent>() const
    requires(Extent != std::dynamic_extent)
    {
        return QualifiedIdentifierBase<std::dynamic_extent>(identifiers | std::ranges::to<std::vector>());
    }

    /// NOLINTNEXTLINE(google-explicit-constructor)
    operator std::span<const Identifier, Extent>() const { return identifiers; }

    [[nodiscard]] constexpr decltype(identifiers.begin()) begin() { return identifiers.begin(); }

    [[nodiscard]] constexpr decltype(identifiers.end()) end() { return identifiers.end(); }

    [[nodiscard]] constexpr decltype(identifiers.cbegin()) begin() const { return identifiers.cbegin(); }

    [[nodiscard]] constexpr decltype(identifiers.cend()) end() const { return identifiers.cend(); }

    [[nodiscard]] constexpr size_t size() const { return identifiers.size(); }

    [[nodiscard]] QualifiedIdentifierBase<std::dynamic_extent> copyAppendLast(const std::ranges::input_range auto&& toAppend) const
    {
        auto newIdentifiers = identifiers | std::ranges::to<std::vector<Identifier>>();
        newIdentifiers.reserve(std::ranges::size(identifiers) + std::ranges::size(toAppend));
        for (const auto& identifier : toAppend)
        {
            newIdentifiers.push_back(identifier);
        }
        return QualifiedIdentifierBase<std::dynamic_extent>{std::move(newIdentifiers)};
    }

    friend std::ostream& operator<<(std::ostream& os, const QualifiedIdentifierBase& obj)
    {
        return os << fmt::format(
                   "{}",
                   fmt::join(
                       obj.identifiers | std::views::transform([](const Identifier& identifier) { return fmt::format("{}", identifier); }),
                       "."));
    }

    template <size_t RhsExtent>
    friend bool operator==(const QualifiedIdentifierBase& lhs, const QualifiedIdentifierBase<RhsExtent>& rhs)
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

    QualifiedIdentifierBase<std::dynamic_extent> operator+(const std::ranges::input_range auto& other) const
    {
        return copyAppendLast(std::move(other));
    }

    QualifiedIdentifierBase<std::dynamic_extent> operator+(const Identifier& other) const
    {
        auto newIdentifiers = identifiers | std::ranges::to<std::vector<Identifier>>();
        newIdentifiers.push_back(other);
        return QualifiedIdentifierBase<std::dynamic_extent>{std::move(newIdentifiers)};
    }

    template <std::ranges::input_range Range>
    requires(std::same_as<std::remove_cv_t<std::ranges::range_value_t<Range>>, Identifier>)
    static std::expected<QualifiedIdentifierBase, Exception> tryCreate(Range identifiers)
    {
        if (std::ranges::empty(identifiers))
        {
            return std::unexpected{InvalidIdentifier("Cannot be empty")};
        }
        if constexpr (Extent != std::dynamic_extent)
        {
            if (std::ranges::size(identifiers) == Extent)
            {
                return std::unexpected{InvalidIdentifier("Expected {} identifiers, but got {}", Extent, std::ranges::size(identifiers))};
            }
            std::array<Identifier, Extent> identifiersArray{};
            std::copy(identifiers.begin(), identifiers.end(), identifiersArray.begin());
            return QualifiedIdentifierBase<Extent>{std::move(identifiersArray)};
        }
        return QualifiedIdentifierBase<std::dynamic_extent>{std::move(identifiers)};
    }

    template <std::ranges::input_range Range>
    requires(
        std::same_as<std::remove_cv_t<std::ranges::range_value_t<Range>>, Identifier>
        && (Extent == std::dynamic_extent || !std::same_as<Range, ContainerType>))
    static QualifiedIdentifierBase create(Range identifiers)
    {
        PRECONDITION(!std::ranges::empty(identifiers), "QualifiedIdentifier must not be empty");
        if constexpr (Extent != std::dynamic_extent)
        {
            PRECONDITION(
                std::ranges::size(identifiers) == Extent, "Expected {} identifiers, but got {}", Extent, std::ranges::size(identifiers));
            std::array<Identifier, Extent> identifiersArray{};
            std::copy(identifiers.begin(), identifiers.end(), identifiersArray.begin());
            return QualifiedIdentifierBase<Extent>{std::move(identifiersArray)};
        }
        else
        {
            return QualifiedIdentifierBase<Extent>{std::move(identifiers)};
        }
    }

    template <size_t CreatedExtent>
    static QualifiedIdentifierBase<CreatedExtent> create(std::array<Identifier, CreatedExtent> identifiers)
    requires(CreatedExtent >= 1)
    {
        return QualifiedIdentifierBase<CreatedExtent>{std::move(identifiers)};
    }

    template <typename... T>
    static QualifiedIdentifierBase<sizeof...(T)> create(const T&... identifiers)
    requires(sizeof...(T) >= 1 && Extent == std::dynamic_extent)
    {
        return QualifiedIdentifierBase<sizeof...(T)>{std::array{Identifier(identifiers)...}};
    }

    static std::expected<QualifiedIdentifierBase, Exception> tryParse(std::string_view name)
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

using QualifiedIdentifier = QualifiedIdentifierBase<std::dynamic_extent>;
static_assert(std::ranges::input_range<std::initializer_list<Identifier>>);

static_assert(std::ranges::input_range<QualifiedIdentifier>);
static_assert(std::ranges::random_access_range<QualifiedIdentifier>);
static_assert(std::ranges::random_access_range<const QualifiedIdentifier>);
static_assert(std::ranges::sized_range<QualifiedIdentifier>);

inline Identifier::operator QualifiedIdentifierBase<1>() const
{
    return QualifiedIdentifierBase<1>{std::array{*this}};
}

inline Identifier::operator QualifiedIdentifierBase<std::dynamic_extent>() const
{
    return QualifiedIdentifier{std::vector{*this}};
}

template <size_t Extent>
struct Reflector<QualifiedIdentifierBase<Extent>>
{
    Reflected operator()(const QualifiedIdentifierBase<Extent>& identifierList) const { return reflect(fmt::format("{}", identifierList)); }
};

template <>
struct Unreflector<QualifiedIdentifier>
{
    QualifiedIdentifier operator()(const Reflected& reflectable, const ReflectionContext& context) const;
};

template <size_t Extent>
requires(Extent != std::dynamic_extent)
struct Unreflector<QualifiedIdentifierBase<Extent>>
{
    QualifiedIdentifierBase<Extent> operator()(const Reflected& reflectable, const ReflectionContext& context) const
    {
        auto idList = context.unreflect<QualifiedIdentifier>(reflectable);
        if (idList.size() != Extent)
        {
            throw CannotDeserialize("Expected an identifier list of size {}, but got {}", Extent, idList.size());
        }
        return QualifiedIdentifierBase<Extent>::create(std::move(idList));
    }
};

}

/// NOLINTBEGIN(cert-dcl58-cpp)
template <size_t Extent>
struct std::hash<NES::QualifiedIdentifierBase<Extent>>
{
    std::size_t operator()(const NES::QualifiedIdentifierBase<Extent>& arg) const noexcept
    {
        return folly::hash::hash_range(std::ranges::begin(arg), std::ranges::end(arg));
    }
};

/// NOLINTEND(cert-dcl58-cpp)

namespace fmt
{
template <>
struct fmt::formatter<NES::QualifiedIdentifierBase<1>> : ostream_formatter
{
};

template <>
struct fmt::formatter<NES::QualifiedIdentifierBase<std::dynamic_extent>> : ostream_formatter
{
};

}

static_assert(fmt::formattable<NES::QualifiedIdentifierBase<std::dynamic_extent>>);
static_assert(fmt::formattable<NES::QualifiedIdentifierBase<1>>);

namespace folly
{
template <>
struct hasher<NES::Identifier>
{
    std::size_t operator()(const NES::Identifier& arg) const noexcept { return std::hash<NES::Identifier>{}(arg); }
};
}
