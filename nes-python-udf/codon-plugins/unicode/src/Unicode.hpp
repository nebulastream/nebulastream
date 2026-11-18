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

#include <cstdint>

/// Unicode properties and case mappings for the INLINE str methods (codon/internal/str.codon), with the results of
/// CPython's str methods: utf8proc provides the general category, the bidirectional class, the simple case mappings and
/// the case folding, and UnicodeTables.hpp, generated from CPython, the rest. utf8proc may know a newer Unicode version
/// than CPython: the tables then decide which characters exist and which are lowercase or uppercase.

/// Bits of nes_unicode_flags.
enum NesUnicodeFlag : uint32_t
{
    NES_UNICODE_ALPHA = 1U << 0U, /// str.isalpha: category L
    NES_UNICODE_DECIMAL = 1U << 1U, /// str.isdecimal: category Nd
    NES_UNICODE_DIGIT = 1U << 2U, /// str.isdigit
    NES_UNICODE_NUMERIC = 1U << 3U, /// str.isnumeric
    NES_UNICODE_SPACE = 1U << 4U, /// str.isspace: bidirectional class WS, B or S, or category Zs
    NES_UNICODE_PRINTABLE = 1U << 5U, /// str.isprintable
    NES_UNICODE_LOWER = 1U << 6U, /// property Lowercase
    NES_UNICODE_UPPER = 1U << 7U, /// property Uppercase
    NES_UNICODE_TITLE = 1U << 8U, /// category Lt
    NES_UNICODE_CASED = 1U << 9U, /// property Cased
    NES_UNICODE_CASE_IGNORABLE = 1U << 10U, /// property Case_Ignorable
    NES_UNICODE_XID_START = 1U << 11U, /// property XID_Start
    NES_UNICODE_XID_CONTINUE = 1U << 12U, /// property XID_Continue
    NES_UNICODE_LINEBREAK = 1U << 13U, /// a line boundary of str.splitlines
};

/// Kinds of nes_unicode_map.
enum NesUnicodeMapping : int64_t
{
    NES_UNICODE_MAP_LOWER = 0,
    NES_UNICODE_MAP_UPPER = 1,
    NES_UNICODE_MAP_TITLE = 2,
    NES_UNICODE_MAP_FOLD = 3,
};

extern "C" uint32_t nes_unicode_flags(int32_t codePoint);
/// Writes the full mapping of the code point (at most 3 code points) to out and returns its length.
extern "C" int64_t nes_unicode_map(int32_t codePoint, int64_t kind, int32_t* out);
/// The value of a decimal digit (category Nd), or -1.
extern "C" int64_t nes_unicode_decimal(int32_t codePoint);
