# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""UDFs for UnicodeStr.test: each applies a group of str operations to its argument and returns the results as one str.
The test runs them IN_PROCESS (CPython) and INLINE (Codon) and expects CPython's results in both. INLINE gets these
results from the str methods of the unicode plugin, which work on code points as CPython's do."""

import re


def _flag(value: bool) -> str:
    return "1" if value else "0"


def indexing(s: str) -> str:
    return " | ".join([
        str(len(s)), s[0], s[-1], s[1:4], s[::2], s[::-1], s[-3:], repr(list(s)[:5]), "".join(reversed(s)),
        str(len(s.encode())),
    ])


def case(s: str) -> str:
    return " | ".join([s.lower(), s.upper(), s.casefold(), s.title(), s.capitalize(), s.swapcase()])


def classes(s: str) -> str:
    words = s.split()
    results = []
    for word in words + [s]:
        results.append("".join([
            _flag(word.isalpha()), _flag(word.isdigit()), _flag(word.isdecimal()), _flag(word.isnumeric()),
            _flag(word.isalnum()), _flag(word.isspace()), _flag(word.isprintable()), _flag(word.islower()),
            _flag(word.isupper()), _flag(word.istitle()), _flag(word.isidentifier()), _flag(word.isascii()),
        ]))
    return " ".join(results)


def search(s: str) -> str:
    probe = s[2:4]
    return " | ".join([
        str(s.find(probe)), str(s.rfind(probe)), str(s.count(s[1])), str(s.find(s[-1], 1)), str(s.index(s[-1])),
        str(s.find("", 3)), str(s.count("")), _flag(s.startswith(s[:2])), _flag(s.endswith(s[-2:])),
        _flag(s.startswith(s[1:3], 1)), _flag(s.endswith(s[:2], 0, 2)), _flag(probe in s), repr(s.partition(s[3])),
        repr(s.rpartition(s[3])),
    ])


def splitting(s: str) -> str:
    return " | ".join([
        repr(s.split()), repr(s.split(None, 1)), repr(s.rsplit(None, 1)), repr(s.split(s[1])), repr(s.splitlines()),
        repr(s.strip()), repr(s.lstrip()), repr(s.rstrip()), repr(s.strip(s[0] + s[-1])), repr(s.strip("")),
    ])


def padding(s: str) -> str:
    return " | ".join([
        s.ljust(24, "·"), s.rjust(24), s.center(25, "*"), s.center(24, "*"), s.zfill(24), ("\t" + s + "\tx").expandtabs(4),
    ])


def characters(s: str) -> str:
    return " | ".join([
        repr(s), repr([ord(c) for c in s][:8]), "".join([chr(ord(c)) for c in s]), s.replace("", "-", 4),
        s.replace(s[1], "XY"), s.translate({ord(s[0]): "Z", ord(s[1]): None}), s * 2, s + "·",
    ])


def regex(s: str) -> str:
    match = re.search(r"\w+", s)
    span = match.span() if match else (-1, -1)
    return " | ".join([
        repr(re.findall(r"\w+", s)), re.sub(r"\s+", "_", s), repr(span), repr(re.split(r"\W+", s)),
        repr(re.findall(r"\d", s)), repr(re.findall(r"[^\s]{2}", s)), repr(re.findall(r"(?i)Σ|ß", s)),
        repr([m.start() for m in re.finditer(r"\S+", s)]), repr(re.findall(r"\w+", s, re.ASCII)),
        # Empty matches between non-ASCII characters, and the pattern of StreamUDFBench's keywords (\b as lookarounds).
        repr(re.findall(r"\w*", s)), repr(re.findall(r"([\d.]+\b|\w+)", s)),
    ])


def code_points(first: int, last: int) -> str:
    """A digest of the character classes, case mappings and repr of each code point in [first, last), without the
    surrogates, which UTF-8 cannot hold."""
    mask = 0x7FFFFFFFFFFFFFFF
    prime = 1099511628211
    digest = 0
    for code_point in range(first, last):
        if 0xD800 <= code_point <= 0xDFFF:
            continue
        c = chr(code_point)
        flags = 0
        for value in [c.isalpha(), c.isdigit(), c.isdecimal(), c.isnumeric(), c.isalnum(), c.isspace(), c.isprintable(),
                      c.islower(), c.isupper(), c.istitle(), c.isidentifier(), ("a" + c).isidentifier(),
                      len(("a" + c + "b").splitlines()) == 2]:
            flags = flags * 2 + (1 if value else 0)
        digest = (digest * prime + flags) & mask
        for d in "\x00".join([c.lower(), c.upper(), c.title(), c.casefold(), c.swapcase(), c.capitalize(), repr(c)]):
            digest = (digest * prime + ord(d)) & mask
    return str(digest)
