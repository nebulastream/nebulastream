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


## bid_strings-shaped rows (ts UINT64, auctionId UINT64, bidder UINT64, price FLOAT64,
## channel VARSIZED, url VARSIZED) for the paper-workload query-shape systests. Prices are
## exact binary fractions, so lazy raw-forward text and materialize-then-serialize agree
## byte-for-byte across the ablation rungs. channel/auctionId/price are chosen so each leg of
## the Q_ex predicate (channel=="Google" AND (auctionId==1001 OR ==1003) AND price<3.5) is the
## single discriminating factor on some row: row 3 fails only on channel, row 4 only on price,
## row 5 only on auctionId; rows 1/6/8 match all three. Row 10's auctionId 984 = 8*123 feeds
## the q2-mod (% 123 == 0) shape. Values avoid every HL7/XML structural byte.
BID_ROWS = [
    (1000, 1001, 21, b"1.5", b"Google", b"https://example.com/a"),
    (2000, 1002, 22, b"4.25", b"Apple", b"https://example.com/b"),
    (3000, 1003, 23, b"2.75", b"Bing", b"https://example.com/c"),
    (4000, 1001, 24, b"5.5", b"Google", b"https://example.com/d"),
    (5000, 1005, 25, b"3.25", b"Google", b"https://example.com/e"),
    (6000, 1003, 26, b"0.5", b"Google", b"https://example.com/f"),
    (7000, 1007, 27, b"6.75", b"Yahoo", b"https://example.com/g"),
    (8000, 1001, 28, b"3.375", b"Google", b"https://example.com/h"),
    (9000, 1009, 29, b"8.125", b"Apple", b"https://example.com/i"),
    (10000, 984, 30, b"9.5", b"Bing", b"https://example.com/j"),
]


def bid_10() -> bytes:
    """10 bid_strings-shaped messages (see BID_ROWS); 6 data fields -> 23 leaves."""
    return b"".join(
        msg(b"OBX|%d|%d|%d|%s|%s|%s" % (ts, auction, bidder, price, channel, url))
        for ts, auction, bidder, price, channel, url in BID_ROWS
    )


if __name__ == "__main__":
    for fname, data in [
        ("two_ints_10.hl7", two_ints_10()),
        ("mixed_types_50.hl7", mixed_types_50()),
        ("arity_bad.hl7", arity_bad()),
        ("bid_10.hl7", bid_10()),
    ]:
        with open(fname, "wb") as f:
            f.write(data)
        print(f"{fname}: {len(data)} bytes, {data.count(FS + CR)} messages")
