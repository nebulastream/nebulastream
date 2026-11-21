# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Dict operations whose result depends on the iteration order. Each function gives the same string in CPython and in
an engine whose dict keeps insertion order, as the language requires since Python 3.7."""

from collections import Counter
import heapq


def _next(state: int) -> int:
    return (state * 6364136223846793005 + 1442695040888963407) & 0xFFFFFFFFFFFF


def fuzz(seed: int) -> str:
    """A seeded sequence of insertions, updates, deletions and pops, with the items after each step that changes the
    order. Keys come from a small range, so a key is often deleted and inserted again."""
    state = _next(seed + 1)
    d = {}
    out = []
    steps = 20 + seed % 200
    keys = 4 + seed % 60
    for step in range(steps):
        state = _next(state)
        op = (state >> 16) % 10
        k = (state >> 24) % keys
        if op < 4:
            d[k] = step
        elif op < 6:
            if k in d:
                del d[k]
        elif op == 6:
            out.append(str(d.pop(k, -1)))
        elif op == 7:
            if d:
                kk, vv = d.popitem()
                out.append(str(kk) + ":" + str(vv))
        elif op == 8:
            out.append(str(d.setdefault(k, -step)))
        else:
            if step % 7 == 0:
                d.clear()
            else:
                d = dict(d)
        out.append(",".join(str(x) for x in d))
    out.append(repr(d))
    out.append(repr(list(d.values())))
    return "|".join(out)


def strings(seed: int) -> str:
    """String keys: insertion, deletion and reinsertion, copy, union, comprehension and fromkeys."""
    words = ["w" + str((seed * 7 + i * 13) % 37) for i in range(30 + seed % 50)]
    d = {}
    for i, w in enumerate(words):
        d[w] = i
    for w in words[::3]:
        if w in d:
            del d[w]
    for w in words[::5]:
        d[w] = -1
    e = d.copy()
    e["new"] = 1
    u = d | {"zz": 0, words[0]: 99}
    c = {k: v * 2 for k, v in d.items() if v % 2 == 0}
    f = dict.fromkeys(words, 0)
    return "|".join([repr(d), repr(e), repr(u), repr(c), repr(list(f))])


def counter(seed: int) -> str:
    """Counter, most_common, heapq.nlargest and a reverse sort by count break ties by first occurrence."""
    words = ["t" + str((seed * 11 + i * 17) % (5 + seed % 23)) for i in range(40 + seed % 60)]
    c = Counter(words)
    top = heapq.nlargest(1 + seed % 6, c, key=c.get)
    by_count = sorted(c.items(), key=lambda t: t[1], reverse=True)
    return "|".join([repr(list(c.items())), repr(c.most_common(4)), repr(c.most_common()), " ".join(top), repr(by_count)])


def sorting(seed: int) -> str:
    """sorted and list.sort are stable, also with reverse=True: (key, position) pairs with many equal keys, of lengths
    that need merges and galloping."""
    state = _next(seed + 7)
    n = 1 + (seed * 53) % 2500
    distinct = 1 + seed % 17
    pairs = []
    for i in range(n):
        state = _next(state)
        if seed % 3 == 0:
            k = (i // (1 + seed % 40)) % distinct  # runs of equal keys
        else:
            k = (state >> 20) % distinct
        pairs.append((k, i))
    up = sorted(pairs, key=lambda p: p[0])
    down = sorted(pairs, key=lambda p: p[0], reverse=True)
    inplace = list(pairs)
    inplace.sort(key=lambda p: -p[0])
    total = 0
    for lst in (up, down, inplace):
        for k, i in lst:
            total = (total * 1000003 + i) & 0xFFFFFFFFFFFF
    return str(n) + ":" + str(total) + ":" + repr(down[:8])


def grow(seed: int) -> str:
    """Many keys, so that the table grows several times, with deletions in between."""
    d = {}
    n = 100 + seed * 37 % 3000
    for i in range(n):
        d[(i * 7919 + seed) % 5003] = i
        if i % 3 == 0:
            d.pop((i * 31 + seed) % 5003, 0)
    keys = list(d)
    total = 0
    for i, k in enumerate(keys):
        total = (total * 31 + k * (i + 1)) & 0xFFFFFFFF
    return str(len(d)) + ":" + str(total) + ":" + ",".join(str(k) for k in keys[:20]) + ":" + ",".join(str(k) for k in keys[-20:])
