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
#include <concepts>
#include <cstddef>
#include <expected>
#include <functional>
#include <initializer_list>
#include <ostream>
#include <ranges>
#include <span>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include <Identifiers/Identifier.hpp>
#include <Util/Reflection.hpp>
#include <fmt/base.h>
#include <fmt/format.h>
#include <fmt/ostream.h>
#include <fmt/ranges.h>
#include <folly/hash/Hash.h>
#include <ErrorHandling.hpp>

namespace NES
{

/// A type representing a qualified identifier, that for Extent > 1 can be multipart, for example MY_SOURCE."attribute_a"
/// If the Extent is not std::dynamic_extent, the QualifiedIdentifier has exactly as many parts as specified in the extent.
/// If it is std::dynamic_extent, it has always at least one.
/// All constructors enforce these invariants.
/// Equality, hashing and fmt::format of course reflect the case sensitivity semantics of Identifiers.
/// Other than that, the type can be used as any range with stable iteration order.
template <size_t Extent>
requires(Extent >= 1)
class QualifiedIdentifierBase
{
    /// Stored container shape depends on whether the extent is fixed at compile time or not.
    /// Declared up-front because it appears in the signature of public members below.
    using ContainerType = std::conditional_t<Extent == std::dynamic_extent, std::vector<Identifier>, std::array<Identifier, Extent>>;

public:
    constexpr explicit QualifiedIdentifierBase(ContainerType identifiers);

    template <std::ranges::input_range Range>
    requires(
        std::same_as<Identifier, std::ranges::range_value_t<Range>> && !std::same_as<Range, ContainerType>
        && (Extent == std::dynamic_extent || std::ranges::sized_range<Range>))
    explicit constexpr QualifiedIdentifierBase(Range&& input) noexcept(Extent < std::dynamic_extent);

    constexpr QualifiedIdentifierBase() = delete;
    constexpr QualifiedIdentifierBase(const QualifiedIdentifierBase& other) = default;
    constexpr QualifiedIdentifierBase(QualifiedIdentifierBase&& other) noexcept = default;
    constexpr QualifiedIdentifierBase& operator=(const QualifiedIdentifierBase& other) = default;
    constexpr QualifiedIdentifierBase& operator=(QualifiedIdentifierBase&& other) noexcept = default;
    constexpr ~QualifiedIdentifierBase() = default;

    /// NOLINTNEXTLINE(google-explicit-constructor)
    constexpr operator const Identifier&() const
    requires(Extent == 1);

    /// NOLINTNEXTLINE(google-explicit-constructor)
    operator QualifiedIdentifierBase<std::dynamic_extent>() const
    requires(Extent != std::dynamic_extent);

    [[nodiscard]] constexpr auto begin() -> decltype(std::declval<ContainerType>().begin());

    [[nodiscard]] constexpr auto end() -> decltype(std::declval<ContainerType>().end());

    [[nodiscard]] constexpr auto begin() const -> decltype(std::declval<ContainerType>().cbegin());

    [[nodiscard]] constexpr auto end() const -> decltype(std::declval<ContainerType>().cend());

    [[nodiscard]] constexpr size_t size() const;

    [[nodiscard]] QualifiedIdentifierBase<std::dynamic_extent> copyAppendLast(const std::ranges::input_range auto&& toAppend) const;

    /// Defined inline as a hidden friend so that ADL finds it across instantiations of QualifiedIdentifierBase.
    friend std::ostream& operator<<(std::ostream& os, const QualifiedIdentifierBase& obj)
    {
        return os << fmt::format(
                   "{}",
                   fmt::join(
                       obj.identifiers | std::views::transform([](const Identifier& identifier) { return fmt::format("{}", identifier); }),
                       "."));
    }

    /// Hidden friend so mixed-extent comparisons (e.g. QualifiedIdentifierBase<1> vs <dynamic_extent>) work via ADL.
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

    QualifiedIdentifierBase<std::dynamic_extent> operator+(const std::ranges::input_range auto& other) const;

    QualifiedIdentifierBase<std::dynamic_extent> operator+(const Identifier& other) const;

    template <std::ranges::input_range Range>
    requires(std::same_as<std::remove_cv_t<std::ranges::range_value_t<Range>>, Identifier>)
    static std::expected<QualifiedIdentifierBase, Exception> tryCreate(Range identifiers);

    template <std::ranges::input_range Range>
    requires(
        std::same_as<Identifier, std::ranges::range_value_t<Range>>
        && (Extent == std::dynamic_extent || (!std::same_as<Range, ContainerType> && std::ranges::sized_range<Range>)))
    static QualifiedIdentifierBase create(Range identifiers);

    template <size_t CreatedExtent>
    static QualifiedIdentifierBase<CreatedExtent> create(std::array<Identifier, CreatedExtent> identifiers)
    requires(CreatedExtent >= 1);

    template <typename... T>
    static QualifiedIdentifierBase<sizeof...(T)> create(const T&... identifiers)
    requires(sizeof...(T) >= 1 && Extent == std::dynamic_extent);

    static std::expected<QualifiedIdentifierBase, Exception> tryParse(std::string_view name)
    requires(Extent == std::dynamic_extent);

    static QualifiedIdentifierBase parse(std::string_view name)
    requires(Extent == std::dynamic_extent);

private:
    ContainerType identifiers;

    template <size_t OtherExtent>
    requires(OtherExtent >= 1)
    friend class QualifiedIdentifierBase;
    friend struct Reflector<QualifiedIdentifierBase>;
    friend struct Unreflector<QualifiedIdentifierBase>;
};

template <size_t Extent>
requires(Extent >= 1)
constexpr QualifiedIdentifierBase<Extent>::QualifiedIdentifierBase(ContainerType identifiers) : identifiers(std::move(identifiers))
{
    PRECONDITION(std::ranges::size(this->identifiers) > 0, "QualifiedIdentifier must not be empty");
}

template <size_t Extent>
requires(Extent >= 1)
template <std::ranges::input_range Range>
requires(
    std::same_as<Identifier, std::ranges::range_value_t<Range>>
    && !std::same_as<Range, typename QualifiedIdentifierBase<Extent>::ContainerType>
    && (Extent == std::dynamic_extent || std::ranges::sized_range<Range>))
constexpr QualifiedIdentifierBase<Extent>::QualifiedIdentifierBase(Range&& input) noexcept(Extent < std::dynamic_extent)
    : identifiers(
          [&]() -> ContainerType
          {
              if constexpr (Extent == std::dynamic_extent)
              {
                  return std::forward<Range>(input) | std::ranges::to<std::vector>();
              }
              else
              {
                  PRECONDITION(std::ranges::size(input) == Extent, "Expected {} identifiers, but got {}", Extent, std::ranges::size(input));
                  std::array<Identifier, Extent> arr{};
                  std::ranges::copy(std::forward<Range>(input), arr.begin());
                  return arr;
              }
          }())
{
    if constexpr (Extent == std::dynamic_extent)
    {
        PRECONDITION(std::ranges::size(this->identifiers) > 0, "QualifiedIdentifier must not be empty");
    }
}

template <size_t Extent>
requires(Extent >= 1)
constexpr QualifiedIdentifierBase<Extent>::operator const Identifier&() const
requires(Extent == 1)
{
    return identifiers[0];
}

template <size_t Extent>
requires(Extent >= 1)
QualifiedIdentifierBase<Extent>::operator QualifiedIdentifierBase<std::dynamic_extent>() const
requires(Extent != std::dynamic_extent)
{
    return QualifiedIdentifierBase<std::dynamic_extent>(identifiers | std::ranges::to<std::vector>());
}

template <size_t Extent>
requires(Extent >= 1)
constexpr auto QualifiedIdentifierBase<Extent>::begin() -> decltype(std::declval<ContainerType>().begin())
{
    return identifiers.begin();
}

template <size_t Extent>
requires(Extent >= 1)
constexpr auto QualifiedIdentifierBase<Extent>::end() -> decltype(std::declval<ContainerType>().end())
{
    return identifiers.end();
}

template <size_t Extent>
requires(Extent >= 1)
constexpr auto QualifiedIdentifierBase<Extent>::begin() const -> decltype(std::declval<ContainerType>().cbegin())
{
    return identifiers.cbegin();
}

template <size_t Extent>
requires(Extent >= 1)
constexpr auto QualifiedIdentifierBase<Extent>::end() const -> decltype(std::declval<ContainerType>().cend())
{
    return identifiers.cend();
}

template <size_t Extent>
requires(Extent >= 1)
constexpr size_t QualifiedIdentifierBase<Extent>::size() const
{
    return identifiers.size();
}

template <size_t Extent>
requires(Extent >= 1)
QualifiedIdentifierBase<std::dynamic_extent>
QualifiedIdentifierBase<Extent>::copyAppendLast(const std::ranges::input_range auto&& toAppend) const
{
    auto newIdentifiers = identifiers | std::ranges::to<std::vector<Identifier>>();
    newIdentifiers.reserve(std::ranges::size(identifiers) + std::ranges::size(toAppend));
    for (const auto& identifier : toAppend)
    {
        newIdentifiers.push_back(identifier);
    }
    return QualifiedIdentifierBase<std::dynamic_extent>{std::move(newIdentifiers)};
}

template <size_t Extent>
requires(Extent >= 1)
QualifiedIdentifierBase<std::dynamic_extent> QualifiedIdentifierBase<Extent>::operator+(const std::ranges::input_range auto& other) const
{
    return copyAppendLast(std::move(other));
}

template <size_t Extent>
requires(Extent >= 1)
QualifiedIdentifierBase<std::dynamic_extent> QualifiedIdentifierBase<Extent>::operator+(const Identifier& other) const
{
    auto newIdentifiers = identifiers | std::ranges::to<std::vector<Identifier>>();
    newIdentifiers.push_back(other);
    return QualifiedIdentifierBase<std::dynamic_extent>{std::move(newIdentifiers)};
}

template <size_t Extent>
requires(Extent >= 1)
template <std::ranges::input_range Range>
requires(std::same_as<std::remove_cv_t<std::ranges::range_value_t<Range>>, Identifier>)
std::expected<QualifiedIdentifierBase<Extent>, Exception> QualifiedIdentifierBase<Extent>::tryCreate(Range identifiers)
{
    if (std::ranges::empty(identifiers))
    {
        return std::unexpected{InvalidIdentifier("Cannot be empty")};
    }
    if constexpr (Extent != std::dynamic_extent)
    {
        if (std::ranges::size(identifiers) != Extent)
        {
            return std::unexpected{InvalidIdentifier("Expected {} identifiers, but got {}", Extent, std::ranges::size(identifiers))};
        }
        std::array<Identifier, Extent> identifiersArray{};
        std::copy(identifiers.begin(), identifiers.end(), identifiersArray.begin());
        return std::expected<QualifiedIdentifierBase<Extent>, Exception>{QualifiedIdentifierBase<Extent>{std::move(identifiersArray)}};
    }
    else if constexpr (Extent == std::dynamic_extent)
    {
        return std::expected<QualifiedIdentifierBase<std::dynamic_extent>, Exception>{
            QualifiedIdentifierBase<std::dynamic_extent>{std::move(identifiers)}};
    }
    std::unreachable();
}

template <size_t Extent>
requires(Extent >= 1)
template <std::ranges::input_range Range>
requires(
    std::same_as<Identifier, std::ranges::range_value_t<Range>>
    && (Extent == std::dynamic_extent
        || (!std::same_as<Range, typename QualifiedIdentifierBase<Extent>::ContainerType> && std::ranges::sized_range<Range>)))
QualifiedIdentifierBase<Extent> QualifiedIdentifierBase<Extent>::create(Range identifiers)
{
    return QualifiedIdentifierBase<Extent>{std::move(identifiers)};
}

template <size_t Extent>
requires(Extent >= 1)
template <size_t CreatedExtent>
QualifiedIdentifierBase<CreatedExtent> QualifiedIdentifierBase<Extent>::create(std::array<Identifier, CreatedExtent> identifiers)
requires(CreatedExtent >= 1)
{
    return QualifiedIdentifierBase<CreatedExtent>{std::move(identifiers)};
}

template <size_t Extent>
requires(Extent >= 1)
template <typename... T>
QualifiedIdentifierBase<sizeof...(T)> QualifiedIdentifierBase<Extent>::create(const T&... identifiers)
requires(sizeof...(T) >= 1 && Extent == std::dynamic_extent)
{
    return QualifiedIdentifierBase<sizeof...(T)>{std::array{Identifier(identifiers)...}};
}

template <size_t Extent>
requires(Extent >= 1)
std::expected<QualifiedIdentifierBase<Extent>, Exception> QualifiedIdentifierBase<Extent>::tryParse(std::string_view name)
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

template <size_t Extent>
requires(Extent >= 1)
QualifiedIdentifierBase<Extent> QualifiedIdentifierBase<Extent>::parse(std::string_view name)
requires(Extent == std::dynamic_extent)
{
    PRECONDITION(!name.empty(), "QualifiedIdentifier must not be empty");
    std::vector<Identifier> identifiers;
    for (const auto& item : name | std::views::split('.'))
    {
        identifiers.push_back(Identifier::parse(std::string{item.begin(), item.end()}));
    }
    return create(std::move(identifiers));
}

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
    Reflected operator()(const QualifiedIdentifierBase<Extent>& identifierList, const ReflectionContext& context) const
    {
        /// Preserve quotes so case-sensitive identifier components survive deserialization.
        return context.reflect(fmt::format(
            "{}",
            fmt::join(
                identifierList
                    | std::views::transform(
                        [](const Identifier& identifier)
                        {
                            return identifier.isCaseSensitive() ? std::string(identifier.getOriginalString())
                                                                : identifier.asCanonicalString();
                        }),
                ".")));
    }
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
