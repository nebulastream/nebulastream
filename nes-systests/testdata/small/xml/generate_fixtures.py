#!/usr/bin/env python3
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""
Generates the committed packed-XML test fixtures in this directory.

Wire format (one packed record per line, no prolog, no pretty-printing, no escaping):
    <rec><id>1</id><val>100</val></rec>\n

These are parsed by the UNMODIFIED HL7 indexers configured for XML (structural class {<,>},
1-byte '\n' message delimiter, component/subcomponent disabled): splitting at every '<' and '>'
yields, for F data elements, arity 4F+5 leaves -- values standalone at slot 4f+4; tag names,
close tags and empties are junk leaves (VARSIZED, never projected, lazy = free).

The record data mirrors the HL7 fixtures (small/hl7/generate_fixtures.py) on purpose: the
XML systests reuse the hand-checked expected outputs of the HL7 systests verbatim.

Run from this directory: python3 generate_fixtures.py
"""


def rec(inner: bytes) -> bytes:
    return b"<rec>" + inner + b"</rec>\n"


def elem(tag: bytes, value: bytes) -> bytes:
    return b"<" + tag + b">" + value + b"</" + tag + b">"


def two_ints_10() -> bytes:
    """10 records, data elements (id INT32, val UINT64) = (i, i*100); F=2 -> arity 13."""
    return b"".join(rec(elem(b"id", b"%d" % i) + elem(b"val", b"%d" % (i * 100))) for i in range(1, 11))


def mixed_types_50() -> bytes:
    """50 records, data elements (id INT32, price FLOAT64, name VARSIZED, url VARSIZED, opt UINT64
    nullable -- empty on every third record); F=5 -> arity 25. The url element is long (~200 chars)
    so records are a sizable fraction of the 4096-byte buffers used by the spanning systest."""
    names = [b"alice", b"bob", b"carol", b"dave", b"eve"]
    out = []
    for i in range(1, 51):
        price = b"%d.%02d" % (i, (i * 7) % 100)
        name = names[i % len(names)]
        url = b"https://example.com/" + bytes([ord("a") + (i % 26)]) * 180
        opt = b"" if i % 3 == 0 else b"%d" % (i * 1000)
        out.append(
            rec(elem(b"id", b"%d" % i) + elem(b"pr", price) + elem(b"nm", name) + elem(b"url", url) + elem(b"opt", opt))
        )
    return b"".join(out)


def arity_bad() -> bytes:
    """3 valid two-ints records, then one with a MISSING element (9 leaves vs 13 -> ERROR 4000)."""
    good = [rec(elem(b"id", b"%d" % i) + elem(b"val", b"%d" % (i * 100))) for i in range(1, 4)]
    bad = rec(elem(b"id", b"4"))  ## only one data element
    return b"".join(good) + bad


def structural_in_value() -> bytes:
    """3 valid two-ints records, then one whose value contains a raw '<' (no escaping/entities in
    this mode: the extra structural byte splits an extra leaf, 14 vs 13 -> ERROR 4000)."""
    good = [rec(elem(b"id", b"%d" % i) + elem(b"val", b"%d" % (i * 100))) for i in range(1, 4)]
    bad = rec(elem(b"id", b"4") + elem(b"val", b"4<00"))
    return b"".join(good) + bad


if __name__ == "__main__":
    for fname, data in [
        ("two_ints_10.xml", two_ints_10()),
        ("mixed_types_50.xml", mixed_types_50()),
        ("arity_bad.xml", arity_bad()),
        ("structural_in_value.xml", structural_in_value()),
    ]:
        with open(fname, "wb") as f:
            f.write(data)
        print(f"{fname}: {len(data)} bytes, {data.count(b'</rec>')} records")
