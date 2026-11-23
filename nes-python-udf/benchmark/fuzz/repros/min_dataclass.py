# codon-fuzz: entry=udf args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["a:b"]]
import dataclasses
from dataclasses import dataclass


@dataclass(order=True)
class P:
    x: str
    n: int = 2


@dataclasses.dataclass
class Q:
    y: str


def udf(s):
    return repr(P(s)) + str(P(s, 1) < P(s, 3)) + str(Q(s) == Q(s)) + repr(Q(s))
