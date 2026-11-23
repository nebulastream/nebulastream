# codon-fuzz: entry=udf args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["a:b"], ["zz"], ["x"]]
from functools import reduce, lru_cache, cache, cmp_to_key
import functools


@lru_cache(maxsize=None)
def up(s):
    return s.upper()


@cache
def twice(s):
    return s + s


@functools.lru_cache
def low(s):
    return s.lower()


def cmp(a, b):
    return len(a) - len(b)


def udf(s):
    parts = [s, "bbb", "c", twice(s)]
    return (reduce(lambda a, b: a + "|" + b, parts) + str(reduce(lambda a, b: a + len(b), parts, 0)) +
            up(s) + low(s) + ",".join(sorted(parts, key=cmp_to_key(cmp))))
