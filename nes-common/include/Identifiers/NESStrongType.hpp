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
#include <compare>
#include <cstddef>
#include <memory>
#include <string>
#include <type_traits>

namespace NES
{
/**
 * Identifiers in NebulaStream are based on a Strong Type. This prevents accidental conversion between different Entity Identifiers.
 * In general a Identifier should not expose its underlying Type and no code should depend on specific values. This is the reason why
 * only a limited subset of operations are supported, and it is not default constructible. Identifiers are orderable and hashable to
 * be used as keys in maps.
 * We Introduce overloads for nlohmann, yaml and fmt to make the identifiers feel ergonomic.
 * @tparam T underlying type
 * @tparam Tag a tag type required to distinguish two strong types
 * @tparam invalid The invalid value
 * @tparam initial The initial value used by Identifier generators
 */
template <typename T, typename Tag, T invalid, T initial>
class NESStrongType
{
public:
    explicit constexpr NESStrongType(T v) : v(v) { }

    using Underlying = T;
    using TypeTag = Tag;
    constexpr static T INITIAL = initial;
    constexpr static T INVALID = invalid;

    [[nodiscard]] friend constexpr std::strong_ordering operator<=>(const NESStrongType& lhs, const NESStrongType& rhs) noexcept = default;

    friend std::ostream& operator<<(std::ostream& os, const NESStrongType& t) { return os << t.getRawValue(); }

    [[nodiscard]] std::string toString() const { return std::to_string(v); }

    /**
     * Serializes the Identifier. Useful for protobuf. Yaml and Json should not require this method.
     * @return the underlying value for serialization purpose
     */
    [[nodiscard]] constexpr T getRawValue() const { return v; }

private:
    T v;
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
}
