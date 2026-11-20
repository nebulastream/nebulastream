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
#include <Unicode.hpp>

#include <algorithm>
#include <array>
#include <cstdint>
#include <span>
#include <UnicodeTables.hpp>
#include <utf8proc.h>

namespace
{
using NES::Unicode::CodePointRange;

bool contains(const std::span<const CodePointRange> ranges, const int32_t codePoint)
{
    const auto range = std::ranges::upper_bound(ranges, codePoint, {}, &CodePointRange::first);
    return range != ranges.begin() && std::prev(range)->last >= codePoint;
}

bool isAssigned(const int32_t codePoint)
{
    return contains(NES::Unicode::assigned, codePoint);
}

/// Word_Break MidLetter, MidNumLet and Single_Quote, which are Case_Ignorable besides the categories Mn, Me, Cf, Lm, Sk.
constexpr std::array<int32_t, 17> wordBreakCaseIgnorable{
    0x27, 0x2E, 0x3A, 0xB7, 0x387, 0x55F, 0x5F4, 0x2018, 0x2019, 0x2024, 0x2027, 0xFE13, 0xFE52, 0xFE55, 0xFF07, 0xFF0E, 0xFF1A};

/// The line boundaries of CPython's str.splitlines.
constexpr std::array<int32_t, 10> lineBreaks{0x0A, 0x0B, 0x0C, 0x0D, 0x1C, 0x1D, 0x1E, 0x85, 0x2028, 0x2029};

uint32_t computeFlags(const int32_t codePoint)
{
    uint32_t flags = 0;
    if (std::ranges::contains(lineBreaks, codePoint))
    {
        flags |= NES_UNICODE_LINEBREAK;
    }
    if (!isAssigned(codePoint))
    {
        return flags;
    }
    const auto category = utf8proc_category(codePoint);
    const auto* const property = utf8proc_get_property(codePoint);
    const auto set = [&](const bool condition, const uint32_t flag) { flags |= condition ? flag : 0U; };

    set(category == UTF8PROC_CATEGORY_LU || category == UTF8PROC_CATEGORY_LL || category == UTF8PROC_CATEGORY_LT
            || category == UTF8PROC_CATEGORY_LM || category == UTF8PROC_CATEGORY_LO,
        NES_UNICODE_ALPHA);
    const bool decimal = category == UTF8PROC_CATEGORY_ND;
    const bool digit = decimal || contains(NES::Unicode::digitBesidesDecimal, codePoint);
    set(decimal, NES_UNICODE_DECIMAL);
    set(digit, NES_UNICODE_DIGIT);
    set(digit || category == UTF8PROC_CATEGORY_NL || category == UTF8PROC_CATEGORY_NO
            || contains(NES::Unicode::numericBesidesCategoryN, codePoint),
        NES_UNICODE_NUMERIC);
    set(property->bidi_class == UTF8PROC_BIDI_CLASS_WS || property->bidi_class == UTF8PROC_BIDI_CLASS_B
            || property->bidi_class == UTF8PROC_BIDI_CLASS_S || category == UTF8PROC_CATEGORY_ZS,
        NES_UNICODE_SPACE);
    set(codePoint == ' '
            || !(
                category == UTF8PROC_CATEGORY_CC || category == UTF8PROC_CATEGORY_CF || category == UTF8PROC_CATEGORY_CS
                || category == UTF8PROC_CATEGORY_CO || category == UTF8PROC_CATEGORY_CN || category == UTF8PROC_CATEGORY_ZL
                || category == UTF8PROC_CATEGORY_ZP || category == UTF8PROC_CATEGORY_ZS),
        NES_UNICODE_PRINTABLE);
    const bool lower = contains(NES::Unicode::lowercase, codePoint);
    const bool upper = contains(NES::Unicode::uppercase, codePoint);
    const bool title = category == UTF8PROC_CATEGORY_LT;
    set(lower, NES_UNICODE_LOWER);
    set(upper, NES_UNICODE_UPPER);
    set(title, NES_UNICODE_TITLE);
    set(lower || upper || title, NES_UNICODE_CASED);
    set(category == UTF8PROC_CATEGORY_MN || category == UTF8PROC_CATEGORY_ME || category == UTF8PROC_CATEGORY_CF
            || category == UTF8PROC_CATEGORY_LM || category == UTF8PROC_CATEGORY_SK
            || std::ranges::contains(wordBreakCaseIgnorable, codePoint),
        NES_UNICODE_CASE_IGNORABLE);
    set(contains(NES::Unicode::xidStart, codePoint), NES_UNICODE_XID_START);
    set(contains(NES::Unicode::xidContinue, codePoint), NES_UNICODE_XID_CONTINUE);
    return flags;
}

/// The flags of the Basic Multilingual Plane, computed once. Other code points are computed at each call.
constexpr int32_t cachedCodePoints = 0x10000;

const std::array<uint32_t, cachedCodePoints>& cachedFlags()
{
    static const auto flags = []
    {
        std::array<uint32_t, cachedCodePoints> result{};
        for (int32_t codePoint = 0; codePoint < cachedCodePoints; ++codePoint)
        {
            result[codePoint] = computeFlags(codePoint);
        }
        return result;
    }();
    return flags;
}
}

/// NOLINTBEGIN(cppcoreguidelines-pro-bounds-pointer-arithmetic)
extern "C" uint32_t nes_unicode_flags(const int32_t codePoint)
{
    if (codePoint >= 0 && codePoint < cachedCodePoints)
    {
        return cachedFlags()[codePoint];
    }
    return codePoint >= 0 && codePoint <= 0x10FFFF ? computeFlags(codePoint) : 0;
}

static_assert(std::ranges::is_sorted(NES::Unicode::specialCasing, {}, &NES::Unicode::SpecialCasing::codePoint));

extern "C" int64_t nes_unicode_map(const int32_t codePoint, const int64_t kind, int32_t* out)
{
    out[0] = codePoint;
    /// ASCII has no special casing, and its case fold is its lowercase.
    if (codePoint >= 0 && codePoint < 0x80)
    {
        const bool toLower = kind == NES_UNICODE_MAP_LOWER || kind == NES_UNICODE_MAP_FOLD;
        if (toLower && codePoint >= 'A' && codePoint <= 'Z')
        {
            out[0] = codePoint + ('a' - 'A');
        }
        else if (!toLower && codePoint >= 'a' && codePoint <= 'z')
        {
            out[0] = codePoint - ('a' - 'A');
        }
        return 1;
    }
    if (!isAssigned(codePoint))
    {
        return 1;
    }
    if (kind == NES_UNICODE_MAP_FOLD)
    {
        std::array<utf8proc_int32_t, 3> folded{};
        int lastBoundClass = 0;
        const auto length = utf8proc_decompose_char(
            codePoint, folded.data(), folded.size(), static_cast<utf8proc_option_t>(UTF8PROC_CASEFOLD), &lastBoundClass);
        if (length <= 0 || length > static_cast<utf8proc_ssize_t>(folded.size()))
        {
            return 1;
        }
        std::ranges::copy_n(folded.begin(), length, out);
        return length;
    }
    for (auto special = std::ranges::lower_bound(NES::Unicode::specialCasing, codePoint, {}, &NES::Unicode::SpecialCasing::codePoint);
         special != NES::Unicode::specialCasing.end() && special->codePoint == codePoint;
         ++special)
    {
        if (special->kind == kind)
        {
            std::ranges::copy_n(special->mapping.begin(), special->length, out);
            return special->length;
        }
    }
    int32_t mapped = codePoint;
    switch (kind)
    {
        case NES_UNICODE_MAP_LOWER:
            mapped = utf8proc_tolower(codePoint);
            break;
        case NES_UNICODE_MAP_UPPER:
            mapped = utf8proc_toupper(codePoint);
            break;
        case NES_UNICODE_MAP_TITLE:
            mapped = utf8proc_totitle(codePoint);
            break;
        default:
            break;
    }
    /// A mapping to a character that a newer Unicode version of utf8proc added, e.g. U+A7D3 to U+A7D2 in Unicode 17, does
    /// not exist in the Unicode version of the tables, so the character keeps its case, as in CPython.
    out[0] = isAssigned(mapped) ? mapped : codePoint;
    return 1;
}

/// NOLINTEND(cppcoreguidelines-pro-bounds-pointer-arithmetic)

extern "C" int64_t nes_unicode_decimal(const int32_t codePoint)
{
    if ((nes_unicode_flags(codePoint) & NES_UNICODE_DECIMAL) == 0U)
    {
        return -1;
    }
    const auto zero = std::ranges::upper_bound(NES::Unicode::decimalZeros, codePoint);
    return zero == NES::Unicode::decimalZeros.begin() ? -1 : codePoint - *std::prev(zero);
}
