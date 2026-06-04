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
#include <cstddef>
#include <expected>
#include <functional>
#include <ostream>
#include <span>
#include <string>
#include <string_view>

#include <Util/Reflection.hpp>
#include <fmt/base.h>
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
public:
    /// NOLINTNEXTLINE(google-explicit-constructor)
    operator QualifiedIdentifierBase<1>() const;

    /// NOLINTNEXTLINE(google-explicit-constructor)
    operator QualifiedIdentifierBase<std::dynamic_extent>() const;

    /// Returns the original string this identifier was created with.
    /// DO NOT USE THIS METHOD FOR COMPARISON.
    /// Does not uppercase non-case sensitive identifiers as normally done.
    /// This method only exists to enable compatibility with systems that follow a different convention.
    [[nodiscard]] std::string_view getOriginalString() const;

    [[nodiscard]] bool isCaseSensitive() const;

    friend std::ostream& operator<<(std::ostream& os, const Identifier& obj);

    [[nodiscard]] std::string asCanonicalString() const;

    friend bool operator==(const Identifier& lhs, const Identifier& rhs);

    static Identifier parse(std::string value);

    static std::expected<Identifier, Exception> tryParse(std::string value);

private:
    explicit Identifier(std::string value, bool caseSensitive);

    Identifier() = default;

    static bool isQuoted(std::string_view value);

    static std::expected<bool, Exception> tryIsQuoted(std::string_view value);

    std::string value;
    bool caseSensitive{};

    friend class IdentifierSerializationUtil;

    template <size_t IdListExtent>
    requires(IdListExtent >= 1)
    friend class QualifiedIdentifierBase;

    friend struct Unreflector<Identifier>;
};

template <>
struct Reflector<Identifier>
{
    Reflected operator()(const Identifier& identifier) const;
};

template <>
struct Unreflector<Identifier>
{
    Identifier operator()(const Reflected& reflectable, const ReflectionContext& context) const;
};
}

template <>
struct fmt::formatter<NES::Identifier>
{
    static constexpr auto parse(const format_parse_context& ctx) -> decltype(ctx.begin()) { return ctx.begin(); }

    [[nodiscard]] static auto format(const NES::Identifier& obj, format_context& ctx) -> decltype(ctx.out());
};

static_assert(fmt::formattable<NES::Identifier>);

/// NOLINTBEGIN(cert-dcl58-cpp)
template <>
struct std::hash<NES::Identifier>
{
    std::size_t operator()(const NES::Identifier& arg) const noexcept;
};

/// NOLINTEND(cert-dcl58-cpp)

/// Needed because folly's default-hasher overloads (e.g. commutative_hash_combine_range used by
/// NES::Hash for std::unordered_map) instantiate folly::hasher<T> for every element type that
/// transitively appears in the input. Without this specialization, anything that hashes a map
/// containing Identifier fails to compile. The body just delegates to std::hash.
namespace folly
{
template <>
struct hasher<NES::Identifier>
{
    std::size_t operator()(const NES::Identifier& arg) const noexcept;
};
}

#include <Identifiers/QualifiedIdentifier.hpp>
