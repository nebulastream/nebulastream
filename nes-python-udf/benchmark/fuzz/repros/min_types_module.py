# codon-fuzz: entry=udf args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["a:b"], ["zz"]]
import types
from types import GeneratorType


def gen(s):
    yield s


def helper(s):
    return s


def udf(s):
    return (str(isinstance(gen(s), GeneratorType)) + str(isinstance(s, types.CodeType)) +
            str(isinstance(s, types.SimpleNamespace)))
