# codon-fuzz: entry=udf args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["ab"]]
from ovtlib.m import helper, Box, kept, added


def udf(s):
    return helper(s) + Box(s).show() + kept(s) + added(s)
