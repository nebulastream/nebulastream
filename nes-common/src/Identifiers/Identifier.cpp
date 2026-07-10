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
#include <fmt/base.h>
#include <fmt/format.h>
#include <ErrorHandling.hpp>

namespace NES
{

Identifier::Identifier(std::string value, const bool caseSensitive) : value(std::move(value)), caseSensitive(caseSensitive)
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
    return value;
}

bool Identifier::isCaseSensitive() const
{
    return caseSensitive;
}

bool operator==(const Identifier& lhs, const Identifier& rhs)
{
    auto sensitive = [](char character) { return character; };
    auto insensitive = [](char character) -> char { return toupper(character); };
    return std::ranges::equal(
        lhs.value, rhs.value, {}, lhs.isCaseSensitive() ? sensitive : insensitive, rhs.isCaseSensitive() ? sensitive : insensitive);
}

Identifier Identifier::parse(std::string value)
{
    PRECONDITION(!value.empty(), "Invalid identifier, cannot be empty");
    const auto caseSensitive = isQuoted(value);
    return Identifier{std::move(value), caseSensitive};
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
    return Identifier{std::move(value), caseSensitive.value()};
}

Reflected Reflector<Identifier>::operator()(const Identifier& identifier) const
{
    return reflect(std::pair{identifier.getOriginalString(), identifier.isCaseSensitive()});
}

Identifier Unreflector<Identifier>::operator()(const Reflected& reflectable, const ReflectionContext& context) const
{
    const auto [val, caseSensitive] = context.unreflect<std::pair<std::string, bool>>(reflectable);
    return Identifier{val, caseSensitive};
}

std::string Identifier::asCanonicalString() const
{
    return fmt::format("{}", *this);
}

std::ostream& operator<<(std::ostream& os, const Identifier& obj)
{
    return os << fmt::format("{}", obj);
}
}

auto fmt::formatter<NES::Identifier>::format(const NES::Identifier& obj, format_context& ctx) -> decltype(ctx.out())
{
    const auto str = obj.getOriginalString();
    if (!obj.isCaseSensitive())
    {
        std::string upper;
        upper.reserve(str.size());
        std::ranges::transform(
            str,
            std::back_inserter(upper),
            [](char character) { return static_cast<char>(std::toupper(static_cast<unsigned char>(character))); });
        return fmt::format_to(ctx.out(), "{}", upper);
    }
    PRECONDITION(str.size() >= 2, "Invalid identifier, cannot be empty");
    return fmt::format_to(ctx.out(), "{}", str.substr(1, str.size() - 2));
}

std::size_t std::hash<NES::Identifier>::operator()(const NES::Identifier& arg) const noexcept
{
    if (arg.isCaseSensitive())
    {
        return std::hash<std::string_view>{}(arg.getOriginalString());
    }

    /// Calculate a hash based on FNV-1 manually, because std::hash does not accept ranges, and we want to avoid the copy.
    constexpr auto FNV_OFFSET_BASIS = 14695981039346656037ULL;
    constexpr auto FNV_PRIME = 1099511628211ULL;
    auto hash = FNV_OFFSET_BASIS;
    for (const auto character : arg.getOriginalString())
    {
        hash ^= std::toupper(character);
        hash *= FNV_PRIME;
    }
    return hash;
}

std::size_t folly::hasher<NES::Identifier>::operator()(const NES::Identifier& arg) const noexcept
{
    return std::hash<NES::Identifier>{}(arg);
}
