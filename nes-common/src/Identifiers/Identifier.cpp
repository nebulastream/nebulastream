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

#include <Identifiers/Identifier.hpp>

#include <algorithm>
#include <cctype>
#include <cstddef>
#include <expected>
#include <functional>
#include <iterator>
#include <ostream>
#include <ranges>
#include <string>
#include <string_view>
#include <utility>

#include <Util/Reflection.hpp>
#include <Util/Strings.hpp>
#include <fmt/base.h>
#include <fmt/format.h>
#include <ErrorHandling.hpp>

namespace NES
{
namespace
{
std::string canonicalizeIdentifier(std::string_view identifierValue, bool quoted)
{
    if (quoted)
    {
        identifierValue.remove_prefix(1);
        identifierValue.remove_suffix(1);
        return std::string(identifierValue);
    }
    return toUpperCase(identifierValue);
}

}

Identifier::Identifier(std::string originalValue, std::string canonicalValue, const bool caseSensitive)
    : originalValue(std::move(originalValue)), canonicalValue(std::move(canonicalValue)), caseSensitive(caseSensitive)
{
}

bool Identifier::isQuoted(std::string_view value)
{
    PRECONDITION(value.find('.') == std::string_view::npos, "Invalid identifier, cannot contain dot: {}", value);
    if (value.front() == '"')
    {
        const auto length = std::ranges::size(value);
        PRECONDITION(length > 1, "Invalid identifier, cannot just contain leading quote: {}", value);
        PRECONDITION(length > 2, "Invalid identifier, cannot just quotes: {}", value);
        PRECONDITION(value.back() == '"', "Invalid identifier, must end with quote: {}", value);
        return true;
    }
    PRECONDITION(value.back() != '"', "Invalid identifier, cannot end with quote if it doesn't start with one: {}", value);
    return false;
}

std::expected<bool, Exception> Identifier::tryIsQuoted(std::string_view value)
{
    if (value.find('.') != std::string_view::npos)
    {
        return std::unexpected{InvalidIdentifier("Cannot contain dot")};
    }
    if (value.front() == '"')
    {
        const auto length = std::ranges::size(value);
        if (length == 1)
        {
            return std::unexpected{InvalidIdentifier("Cannot just contain leading quote")};
        }
        if (length == 2)
        {
            return std::unexpected{InvalidIdentifier("Cannot just quotes")};
        }
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

std::string_view Identifier::getOriginalString() const
{
    return originalValue;
}

bool Identifier::isCaseSensitive() const
{
    return caseSensitive;
}

bool operator==(const Identifier& lhs, const Identifier& rhs)
{
    return lhs.canonicalValue == rhs.canonicalValue;
}

Identifier Identifier::parse(std::string value)
{
    PRECONDITION(!value.empty(), "Invalid identifier, cannot be empty");
    const auto caseSensitive = isQuoted(value);
    auto canonicalValue = canonicalizeIdentifier(value, caseSensitive);
    return Identifier{std::move(value), std::move(canonicalValue), caseSensitive};
}

std::expected<Identifier, Exception> Identifier::tryParse(std::string value)
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
    auto canonicalValue = canonicalizeIdentifier(value, caseSensitive.value());
    return Identifier{std::move(value), std::move(canonicalValue), caseSensitive.value()};
}

Reflected Reflector<Identifier>::operator()(const Identifier& identifier, const ReflectionContext& context) const
{
    return context.reflect(std::pair{std::pair{identifier.originalValue, identifier.canonicalValue}, identifier.caseSensitive});
}

Identifier Unreflector<Identifier>::operator()(const Reflected& reflectable, const ReflectionContext& context) const
{
    const auto [strings, caseSensitive] = context.unreflect<std::pair<std::pair<std::string, std::string>, bool>>(reflectable);
    const auto [original, canonicalized] = strings;
    return Identifier{original, canonicalized, caseSensitive};
}

std::string Identifier::asCanonicalString() const
{
    return canonicalValue;
}

std::ostream& operator<<(std::ostream& os, const Identifier& obj)
{
    return os << fmt::format("{}", obj);
}
}

auto fmt::formatter<NES::Identifier>::format(const NES::Identifier& obj, format_context& ctx) -> decltype(ctx.out())
{
    return fmt::format_to(ctx.out(), "{}", obj.canonicalValue);
}

std::size_t std::hash<NES::Identifier>::operator()(const NES::Identifier& arg) const noexcept
{
    return std::hash<std::string_view>{}(arg.canonicalValue);
}

std::size_t folly::hasher<NES::Identifier>::operator()(const NES::Identifier& arg) const noexcept
{
    return std::hash<NES::Identifier>{}(arg);
}
