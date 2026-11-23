# codon-fuzz: entry=udf args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["new york"], ["SAN FRANCISCO"], ["fort lauderdale"], ["st. louis"], ["dallas/fort worth"], ["  salt   lake city "]]
# real-udf: origin=literature id=tuplex_flights_capwords requires= wrapped=0 ported=0
# Wrapper: The benchmark script imports string at its top; the case adds the import before the UDF code.
# Inputs: an empty string returns None (NULL); the rows avoid it.
import string

# Source: Tuplex: Data Science in Python at Native Code Speed (Spiegelberg et al., SIGMOD 2021)
# URL: https://github.com/tuplex/tuplex/blob/5380aec8ab38017401610435511450cda9fd8b3b/benchmarks/flights/runtuplex.py#L201
# Repository: https://github.com/tuplex/tuplex at commit 5380aec8ab38017401610435511450cda9fd8b3b
# License: Apache-2.0
# Path: benchmarks/flights/runtuplex.py, lines 201-201
# Note: the lambda text is verbatim; the assignment 'udf = ' is added so the file is valid Python.

udf = lambda x: string.capwords(x) if x else None
