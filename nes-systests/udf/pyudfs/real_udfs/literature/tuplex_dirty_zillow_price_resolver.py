# codon-fuzz: entry=case_entry args=VARSIZED returns=INT64
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["$685,000"], ["$1,250,000"], ["$2,450/mo"], ["$1,800+/mo"], ["$999"], ["$2,100+/mo"]]
# real-udf: origin=literature id=tuplex_dirty_zillow_price_resolver requires= wrapped=1 ported=0
# Wrapper: Tuplex passes the row as a dict; case_entry builds it from the fields the UDF reads.
# Wrapper: The benchmark script imports re at its top; the case adds the import before the UDF code.
import re

# Source: Tuplex: Data Science in Python at Native Code Speed (Spiegelberg et al., SIGMOD 2021)
# URL: https://github.com/tuplex/tuplex/blob/5380aec8ab38017401610435511450cda9fd8b3b/benchmarks/dirty_zillow/runtuplex.py#L257
# Repository: https://github.com/tuplex/tuplex at commit 5380aec8ab38017401610435511450cda9fd8b3b
# License: Apache-2.0
# Path: benchmarks/dirty_zillow/runtuplex.py, lines 257-257
# Note: the lambda text is verbatim; the assignment 'udf = ' is added so the file is valid Python.

udf = lambda x: int(re.sub('[^0-9.]*', '', x['price']))


def case_entry(price):
    return udf({"price": price})
