# codon-fuzz: entry=udf args=VARSIZED returns=INT64
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["1996-03-13"], ["1994-01-01"], ["1995-12-31"], ["1992-06-28"], ["1998-08-02"], ["1994-11-30"]]
# real-udf: origin=literature id=tuplex_tpch_q06_shipdate_to_int requires= wrapped=0 ported=0

# Source: Tuplex: Data Science in Python at Native Code Speed (Spiegelberg et al., SIGMOD 2021)
# URL: https://github.com/tuplex/tuplex/blob/5380aec8ab38017401610435511450cda9fd8b3b/benchmarks/tpch/Q06/runtuplex.py#L77
# Repository: https://github.com/tuplex/tuplex at commit 5380aec8ab38017401610435511450cda9fd8b3b
# License: Apache-2.0
# Path: benchmarks/tpch/Q06/runtuplex.py, lines 77-77
# Note: the lambda text is verbatim; the assignment 'udf = ' is added so the file is valid Python.

udf = lambda x: int(x.replace('-', ''))
