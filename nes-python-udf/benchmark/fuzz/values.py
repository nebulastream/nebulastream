"""Input values for the differential tests of Codon against CPython: edge cases per NES type plus seeded random values.

The edge cases are values where Codon's own implementation of the builtins can differ from CPython: Unicode case mappings
and whitespace, non-ASCII digits, integer boundaries, signed zero, NaN, infinities, subnormals and halfway cases.
"""

import random

INT64_MIN = -(2**63)
INT64_MAX = 2**63 - 1

EDGE_STRINGS = [
    "", " ", "a", "A", "abc", "Hello World", "hello world", "HELLO", "hElLo wOrLd", "  padded  ", "\t tab\t", "a\nb",
    "a\r\nb", "a\x0bb\x0cc", "x\x1cy\x1dz\x1ew\x1fv", "line sep par", "nbsp here", "ideo　space",
    "next\x85line", "0", "-0", "+7", "42", " 42 ", "-17", "3.14", "-2.5e3", "1_000", "0x1f", "1e400", "nan", "inf",
    "-Infinity", "٣٤", "²³", "⅕", "１２", "०", "Straße", "STRASSE",
    "İstanbul", "ı", "Σασ", "ΟΔΟΣ", "ẞ", "ﬁne", "ǅungla",
    "Ǆ", "café", "café", "Ångström", "Ω", "naïve", "և", "ᾈ",
    "\U0001d400\U0001d401", "\U0001f600 smile", "\U0001f1e9\U0001f1ea", "日本語", "한국어",
    "שלום", "مرحبا", "a,b,,c", ",", ",,", "a b  c   d", "aaa", "abab",
    "x" * 100, "ab" * 300, "The quick brown fox jumps over the lazy dog", "it's o'neil-smith", "under_score_word",
    "CamelCaseWord", "{\"a\": 1, \"b\": [1, 2]}", "[1, 2, 3]", "\\backslash\\", "quote\"s", "%s %d", "{0}", "tab\there",
    "\x00nul", "\x7fdel", "1/2/2020", "2020-01-31", "  ", "​", "­", "﻿bom",
]

EDGE_INTS = [
    0, 1, -1, 2, -2, 3, 7, 10, -10, 31, 32, 63, 64, 100, 255, 256, -129, 1000, 10**9, -(10**9), 2**31 - 1, -(2**31),
    2**31, 2**32, 2**53, 2**53 + 1, 10**18, -(10**18), INT64_MAX, INT64_MIN, INT64_MAX - 1, INT64_MIN + 1,
]

EDGE_FLOATS = [
    0.0, -0.0, 1.0, -1.0, 0.5, -0.5, 1.5, 2.5, -2.5, 0.1, 0.2, 0.3, 1 / 3, 2 / 3, 3.14159, 1e-7, 1e-5, 1e-4, 1e15, 1e16,
    1e17, 1e22, 1e23, 1e100, 1e308, 1.7976931348623157e308, 5e-324, 2.2250738585072014e-308, -1e-300, 123456789.125,
    0.125, 1.005, 2.675, 4.35, 99.995, 1234.5, -1234.5, 2.0**53, 2.0**63, -(2.0**63), 2.0**64, float("inf"),
    float("-inf"), float("nan"),
]

ALPHABETS = [
    "abcdefghijklmnopqrstuvwxyz", "ABCDEFGHIJKLMNOPQRSTUVWXYZ", "0123456789", " \t\n,.;:-_/'\"!?()[]{}",
    "äöüßÄÖÜéèêçñ",
    "αβγσςΣΑΒİıǅẞﬁ",
    "٠١٢۱०０²⅕Ⅰ",
    "  　\x85\x1c​", "日本\U0001f600\U0001d400́",
]


def random_string(rng):
    length = rng.choice([0, 1, 2, 3, 5, 8, 13, 30, 80])
    pools = rng.sample(ALPHABETS, rng.randint(1, 3))
    alphabet = "".join(pools)
    return "".join(rng.choice(alphabet) for _ in range(length))


def random_int(rng):
    kind = rng.random()
    if kind < 0.4:
        return rng.randint(-100, 100)
    if kind < 0.7:
        return rng.randint(-(10**6), 10**6)
    if kind < 0.9:
        return rng.randint(-(2**40), 2**40)
    return rng.randint(INT64_MIN, INT64_MAX)


def random_float(rng):
    kind = rng.random()
    if kind < 0.3:
        return round(rng.uniform(-100, 100), rng.randint(0, 4))
    if kind < 0.6:
        return rng.uniform(-1e6, 1e6)
    if kind < 0.8:
        return rng.choice([-1, 1]) * 10.0 ** rng.uniform(-320, 308)
    return rng.choice(EDGE_FLOATS) * rng.choice([1, -1, 0.5, 2, 10])


EDGES = {"VARSIZED": EDGE_STRINGS, "INT64": EDGE_INTS, "INT32": [v for v in EDGE_INTS if -(2**31) <= v < 2**31],
         "FLOAT64": EDGE_FLOATS, "BOOLEAN": [True, False]}
RANDOM = {"VARSIZED": random_string, "INT64": random_int,
          "INT32": lambda rng: max(-(2**31), min(2**31 - 1, random_int(rng))), "FLOAT64": random_float,
          "BOOLEAN": lambda rng: rng.random() < 0.5}


def rows(types, count, seed, null_fraction=0.0):
    """Argument rows for the NES types: every edge value of each argument once, then random combinations."""
    rng = random.Random(seed)
    result = []
    longest = max(len(EDGES[t]) for t in types)
    for index in range(longest):
        result.append([EDGES[t][(index * (position + 1)) % len(EDGES[t])] for position, t in enumerate(types)])
    while len(result) < count:
        row = []
        for t in types:
            choice = rng.random()
            if choice < null_fraction:
                row.append(None)
            elif choice < 0.35:
                row.append(rng.choice(EDGES[t]))
            else:
                row.append(RANDOM[t](rng))
        result.append(row)
    return result[:max(count, longest)]
