# codon-fuzz: entry=udf args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 3
import re


def udf(s):
    return s + "\N{GREEK SMALL LETTER ALPHA}"
