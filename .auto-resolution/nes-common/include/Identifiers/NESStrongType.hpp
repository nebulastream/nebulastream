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
#include <compare>
#include <concepts>
#include <cstddef>
#include <span>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <Util/UUID.hpp>

namespace NES
{
/// Identifiers in NebulaStream are based on a Strong Type. This prevents accidental conversion between different Entity Identifiers.
/// In general a Identifier should not expose its underlying Type and no code should depend on specific values. This is the reason why
/// only a limited subset of operations are supported, and it is not default constructible. Identifiers are orderable and hashable to
/// be used as keys in maps.
/// We Introduce overloads for nlohmann, yaml and fmt to make the identifiers feel ergonomic.
/// @tparam T underlying type
/// @tparam Tag a tag type required to distinguish two strong types
/// @tparam invalid The invalid value
/// @tparam initial The initial value used by Identifier generators
template <typename T, typename Tag, T invalid, T initial>
class NESStrongType
{
public:
    explicit constexpr NESStrongType(T value) : value(value) { }

    using Underlying = T;
    using TypeTag = Tag;
    constexpr static T INITIAL = initial;
    constexpr static T INVALID = invalid;

    [[nodiscard]] friend constexpr std::strong_ordering operator<=>(const NESStrongType& lhs, const NESStrongType& rhs) noexcept = default;

    friend std::ostream& operator<<(std::ostream& os, const NESStrongType& t) { return os << t.getRawValue(); }

    [[nodiscard]] std::string toString() const { return std::to_string(value); }

    /// return the underlying value as a value of the underlying type
    [[nodiscard]] constexpr T getRawValue() const { return value; }

private:
    T value;
};

template <size_t N>
struct StringLiteral
{
    /// C-Strings cannot be converted to std::array, so we are using a fixed size char array where the length N can be deduced.
    /// StringLiteral is intended to be used as a non-type template parameter like fun<"my_string"> so we want the non-explicit constructor.
    ///NOLINTNEXTLINE(modernize-avoid-c-arrays, google-explicit-constructor)
    constexpr StringLiteral(const char (&str)[N]) { std::copy_n(std::span<const char, N>{str}.begin(), N, value.begin()); }

    std::array<char, N> value;
};

template <typename Tag, StringLiteral Invalid>
class NESStrongStringType
{
    std::string value;

public:
    using Underlying = std::string;
    using TypeTag = Tag;
    static constexpr std::string_view INVALID{Invalid.value.begin(), Invalid.value.end()};

    explicit constexpr NESStrongStringType(std::string_view view) : value(std::string(view)) { }

    template <std::convertible_to<std::string> StringType>
    explicit constexpr NESStrongStringType(StringType&& stringType)
    {
        if constexpr (std::is_same_v<std::decay_t<StringType>, std::string>)
        {
            value = std::forward<StringType>(stringType);
        }
        else if constexpr (std::is_convertible_v<StringType, std::string_view>)
        {
            ///NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
            value = std::string_view(std::forward<StringType>(stringType));
        }
        else
        {
            value = static_cast<std::string>(std::forward<StringType>(stringType));
        }
    }

    [[nodiscard]] friend constexpr std::strong_ordering operator<=>(const NESStrongStringType& lhs, const NESStrongStringType& rhs) noexcept
        = default;

    friend std::ostream& operator<<(std::ostream& os, const NESStrongStringType& t) { return os << t.value; }

    [[nodiscard]] std::string getRawValue() const { return value; }

    [[nodiscard]] std::string_view view() const { return value; }
};

template <typename Tag>
class NESStrongUUIDType
{
    UUID value;

public:
    using Underlying = std::string;
    using TypeTag = Tag;
    constexpr static UUID INVALID{};

    explicit constexpr NESStrongUUIDType(const std::string& stringUUID) : value(stringToUUIDOrThrow(stringUUID)) { }

    explicit constexpr NESStrongUUIDType(UUID uuid) : value(std::move(uuid)) { }

    explicit constexpr NESStrongUUIDType(std::string_view view) : NESStrongUUIDType(std::string(view)) { }

    [[nodiscard]] friend constexpr std::strong_ordering operator<=>(const NESStrongUUIDType& lhs, const NESStrongUUIDType& rhs) noexcept
        = default;

    friend std::ostream& operator<<(std::ostream& os, const NESStrongUUIDType& strongType) { return os << strongType.getRawValue(); }

    [[nodiscard]] std::string getRawValue() const { return UUIDToString(value); }

    [[nodiscard]] const UUID& view() const { return value; }
};

template <typename T>
concept NESIdentifier = requires(T t) {
    requires(std::same_as<T, NESStrongType<typename T::Underlying, typename T::TypeTag, T::INVALID, T::INITIAL>>);
    requires(!std::is_default_constructible_v<T>);
    requires(std::is_trivially_copyable_v<T>);
    requires(sizeof(t) == sizeof(typename T::Underlying));
    requires(!std::is_convertible_v<T, typename T::Underlying>);
    requires(std::is_trivially_destructible_v<T>);
    { t < t };
    { t > t };
    { t == t };
    { t != t };
    { std::hash<T>()(t) };
};

template <NESIdentifier Ident>
static constexpr Ident INVALID = Ident(Ident::INVALID);
template <NESIdentifier Ident>
static constexpr Ident INITIAL = Ident(Ident::INITIAL);

}

namespace std
{
template <typename T, typename Tag, T invalid, T initial>
struct hash<NES::NESStrongType<T, Tag, invalid, initial>>
{
    size_t operator()(const NES::NESStrongType<T, Tag, invalid, initial>& strongType) const
    {
        return std::hash<T>()(strongType.getRawValue());
    }
};

template <typename Tag, NES::StringLiteral Invalid>
struct hash<NES::NESStrongStringType<Tag, Invalid>>
{
    size_t operator()(const NES::NESStrongStringType<Tag, Invalid>& strongType) const
    {
        return std::hash<std::string>()(strongType.getRawValue());
    }
};

template <typename Tag>
struct hash<NES::NESStrongUUIDType<Tag>>
{
    size_t operator()(const NES::NESStrongUUIDType<Tag>& strongType) const { return std::hash<NES::UUID>()(strongType.view()); }
};
}
