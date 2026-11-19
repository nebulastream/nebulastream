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
"""frozenset, collections.Counter and heapq.nlargest for nopython mode.

frozenset(iterable) is a set: Numba has no immutable set, and the UDFs only test membership. Counter(iterable of strings) is a
typed dict of counts in insertion order. heapq.nlargest(n, counter, key=counter.get) returns the n keys with the largest
counts, and among equal counts the earlier inserted key first, as CPython's heapq.nlargest does with a key."""

import collections
import heapq

from numba import types
from numba.core.extending import overload
from numba.typed import Dict


@overload(frozenset)
def _frozenset(iterable):
    return lambda iterable: set(iterable)


@overload(collections.Counter)
def _counter(iterable):
    if isinstance(iterable, (types.List, types.ListType)) and isinstance(iterable.dtype, types.UnicodeType):
        def implementation(iterable):
            counts = Dict.empty(types.unicode_type, types.int64)
            for item in iterable:
                counts[item] = counts.get(item, 0) + 1
            return counts
        return implementation
    return None


@overload(heapq.nlargest)
def _nlargest(n, iterable, key=None):
    is_counts = isinstance(iterable, types.DictType) and isinstance(iterable.key_type, types.UnicodeType)
    # The key must be counts.get; Numba types the bound method only by its dictionary type, so this checks the kind.
    is_get = isinstance(key, types.BoundFunction)
    if not (is_counts and is_get):
        return None

    def implementation(n, iterable, key=None):
        ranked = []
        index = 0
        for word, count in iterable.items():
            ranked.append((-count, index, word))
            index += 1
        ranked.sort()
        result = [""]
        result.pop()
        for position in range(min(n, len(ranked))):
            result.append(ranked[position][2])
        return result

    return implementation
