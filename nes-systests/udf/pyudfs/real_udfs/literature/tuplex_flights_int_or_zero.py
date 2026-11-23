# codon-fuzz: entry=udf args=FLOAT64 returns=INT64
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [[0.0], [14.0], [3.0], [127.0], [0.0], [52.0], [-0.0]]
# real-udf: origin=literature id=tuplex_flights_int_or_zero requires= wrapped=0 ported=0

# Source: Tuplex: Data Science in Python at Native Code Speed (Spiegelberg et al., SIGMOD 2021)
# URL: https://github.com/tuplex/tuplex/blob/5380aec8ab38017401610435511450cda9fd8b3b/benchmarks/flights/runtuplex.py#L274
# Repository: https://github.com/tuplex/tuplex at commit 5380aec8ab38017401610435511450cda9fd8b3b
# License: Apache-2.0
# Path: benchmarks/flights/runtuplex.py, lines 274-274
# Note: the lambda text is verbatim; the assignment 'udf = ' is added so the file is valid Python.

udf = lambda x: int(x) if x else 0
