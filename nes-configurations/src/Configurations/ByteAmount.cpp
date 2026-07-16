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

#include <Configurations/ByteAmount.hpp>

#include <array>
#include <cctype>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <optional>
#include <string>
#include <string_view>
#include <ErrorHandling.hpp>

namespace NES
{
namespace
{
/// 128-bit intermediates make the overflow checks trivial: no legal input can overflow them (see the static_asserts below).
using UInt128 = unsigned __int128;

constexpr uint64_t DECIMAL_BASE = 1000;
constexpr uint64_t BINARY_BASE = 1024;
/// Bounding the fraction keeps `fractionDigits * multiplier` within 128 bits (10^20 * 2^60 < 2^127) and loses nothing in practice:
/// with the largest multiplier below 10^19, more than 20 significant fractional digits (almost) never scale to whole bytes.
constexpr size_t MAX_FRACTIONAL_DIGITS = 20;

constexpr uint64_t pow(const uint64_t base, const uint64_t exponent)
{
    uint64_t result = 1;
    for (uint64_t i = 0; i < exponent; ++i)
    {
        result *= base;
    }
    return result;
}

/// Returns the multiplier for a byte-amount suffix (lowercased, optional trailing 'b' already stripped), or nullopt if unknown.
std::optional<uint64_t> multiplierForSuffix(const std::string_view suffix)
{
    struct SuffixMultiplier
    {
        std::string_view suffix;
        uint64_t multiplier;
    };

    static constexpr std::array<SuffixMultiplier, 13> SUFFIXES{
        {{"", 1},
         {"k", pow(DECIMAL_BASE, 1)},
         {"m", pow(DECIMAL_BASE, 2)},
         {"g", pow(DECIMAL_BASE, 3)},
         {"t", pow(DECIMAL_BASE, 4)},
         {"p", pow(DECIMAL_BASE, 5)},
         {"e", pow(DECIMAL_BASE, 6)},
         {"ki", pow(BINARY_BASE, 1)},
         {"mi", pow(BINARY_BASE, 2)},
         {"gi", pow(BINARY_BASE, 3)},
         {"ti", pow(BINARY_BASE, 4)},
         {"pi", pow(BINARY_BASE, 5)},
         {"ei", pow(BINARY_BASE, 6)}}};
    static_assert(pow(BINARY_BASE, 6) == UInt128{1} << 60);
    static_assert(pow(DECIMAL_BASE, 6) < UInt128{1} << 60);

    for (const auto& [candidate, multiplier] : SUFFIXES)
    {
        if (candidate == suffix)
        {
            return multiplier;
        }
    }
    return std::nullopt;
}

std::string_view takeDigits(std::string_view& rest)
{
    size_t count = 0;
    while (count < rest.size() && (std::isdigit(static_cast<unsigned char>(rest[count])) != 0))
    {
        ++count;
    }
    const auto digits = rest.substr(0, count);
    rest.remove_prefix(count);
    return digits;
}
}

std::expected<uint64_t, Exception> parseByteAmount(const std::string_view amount)
{
    const auto invalid = [&](const std::string_view reason)
    { return std::unexpected(InvalidConfigParameter("Invalid byte amount \"{}\": {}", amount, reason)); };

    if (amount.empty())
    {
        return invalid("empty value");
    }
    if (amount.front() == '-' || amount.front() == '+')
    {
        return invalid("signed values are not supported");
    }

    auto rest = amount;
    const auto integerDigits = takeDigits(rest);
    auto fractionalDigits = std::string_view{};
    if (!rest.empty() && rest.front() == '.')
    {
        rest.remove_prefix(1);
        fractionalDigits = takeDigits(rest);
        if (fractionalDigits.empty())
        {
            return invalid("expected digits after the decimal point");
        }
    }
    if (integerDigits.empty() && fractionalDigits.empty())
    {
        return invalid("expected a number before the unit suffix");
    }

    /// Match the unit suffix case-insensitively, ignoring an optional trailing 'B' ("4KiB" == "4Ki", "4kb" == "4k").
    std::string suffix{rest};
    for (auto& character : suffix)
    {
        character = static_cast<char>(std::tolower(static_cast<unsigned char>(character)));
    }
    if (!suffix.empty() && suffix.back() == 'b')
    {
        suffix.pop_back();
    }
    const auto multiplier = multiplierForSuffix(suffix);
    if (!multiplier.has_value())
    {
        return invalid("unknown unit suffix");
    }

    UInt128 integerPart = 0;
    for (const char digit : integerDigits)
    {
        integerPart = integerPart * 10 + static_cast<UInt128>(digit - '0');
        if (integerPart > UINT64_MAX)
        {
            return invalid("value exceeds the maximum of 2^64 - 1 bytes");
        }
    }
    /// integerPart <= 2^64 - 1 and multiplier <= 2^60, so the product stays well within 128 bits.
    UInt128 result = integerPart * static_cast<UInt128>(multiplier.value());

    /// Trailing zeros carry no value, dropping them widens the accepted range of (pointless but valid) high-precision fractions.
    while (!fractionalDigits.empty() && fractionalDigits.back() == '0')
    {
        fractionalDigits.remove_suffix(1);
    }
    if (!fractionalDigits.empty())
    {
        if (fractionalDigits.size() > MAX_FRACTIONAL_DIGITS)
        {
            return invalid("too many fractional digits");
        }
        UInt128 fraction = 0;
        UInt128 fractionScale = 1;
        for (const char digit : fractionalDigits)
        {
            fraction = fraction * 10 + static_cast<UInt128>(digit - '0');
            fractionScale *= 10;
        }
        /// fraction < 10^20 < 2^67 and multiplier <= 2^60, so the product stays within 128 bits.
        const UInt128 scaledFraction = fraction * static_cast<UInt128>(multiplier.value());
        if (scaledFraction % fractionScale != 0)
        {
            return invalid("does not resolve to a whole number of bytes");
        }
        result += scaledFraction / fractionScale;
    }

    if (result > UINT64_MAX)
    {
        return invalid("value exceeds the maximum of 2^64 - 1 bytes");
    }
    return static_cast<uint64_t>(result);
}
}
