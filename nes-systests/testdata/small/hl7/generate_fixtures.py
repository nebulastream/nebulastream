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
Generates the committed HL7 test fixtures in this directory.

Wire format (one MLLP-framed message per record, produced by the HL7 output formatter):
    <VT=0x0B> MSH_HEADER <CR> SEGNAME|f1|...|fn <CR> <FS=0x1C><CR>

The fixtures are generated INDEPENDENTLY of the engine's HL7 output formatter on purpose:
the HL7 input-indexer systests that read them are then not circular (the engine-vs-engine
loop closure lives in the round-trip systest instead).

Run from this directory: python3 generate_fixtures.py
"""

HDR = b"MSH|^~\\&|NES|NES|BENCH|BENCH|20260101000000||ORU^R01|1|P|2.5"
VT, CR, FS = b"\x0b", b"\r", b"\x1c"


def msg(segment: bytes, header: bytes = HDR) -> bytes:
    return VT + header + CR + segment + CR + FS + CR


def two_ints_10() -> bytes:
    """10 messages, data fields (id INT32, val UINT64) = (i, i*100)."""
    return b"".join(msg(b"OBX|%d|%d" % (i, i * 100)) for i in range(1, 11))


def mixed_types_50() -> bytes:
    """50 messages, data fields (id INT32, price FLOAT64, name VARSIZED, url VARSIZED, opt UINT64
    nullable -- empty on every third message). The url field is long (~200 chars) so messages are a
    sizable fraction of the 4096-byte buffers used by the spanning systest."""
    names = [b"alice", b"bob", b"carol", b"dave", b"eve"]
    out = []
    for i in range(1, 51):
        price = b"%d.%02d" % (i, (i * 7) % 100)
        name = names[i % len(names)]
        url = b"https://example.com/" + bytes([ord("a") + (i % 26)]) * 180
        opt = b"" if i % 3 == 0 else b"%d" % (i * 1000)
        out.append(msg(b"OBX|%d|%s|%s|%s|%s" % (i, price, name, url, opt)))
    return b"".join(out)


def arity_bad() -> bytes:
    """3 valid two-ints messages, then one with a MISSING field (arity mismatch -> ERROR 4000)."""
    good = [msg(b"OBX|%d|%d" % (i, i * 100)) for i in range(1, 4)]
    bad = msg(b"OBX|4")  ## only one data field
    return b"".join(good) + bad


if __name__ == "__main__":
    for fname, data in [
        ("two_ints_10.hl7", two_ints_10()),
        ("mixed_types_50.hl7", mixed_types_50()),
        ("arity_bad.hl7", arity_bad()),
    ]:
        with open(fname, "wb") as f:
            f.write(data)
        print(f"{fname}: {len(data)} bytes, {data.count(FS + CR)} messages")
