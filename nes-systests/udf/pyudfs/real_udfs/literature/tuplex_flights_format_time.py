# codon-fuzz: entry=udf args=INT64 returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [[1435], [5], [2359], [700], [1201], [45], [2400]]
# real-udf: origin=literature id=tuplex_flights_format_time requires= wrapped=0 ported=0
# Inputs: 0 returns None (NULL); the rows avoid it.

# Source: Tuplex: Data Science in Python at Native Code Speed (Spiegelberg et al., SIGMOD 2021)
# URL: https://github.com/tuplex/tuplex/blob/5380aec8ab38017401610435511450cda9fd8b3b/benchmarks/flights/runtuplex.py#L142
# Repository: https://github.com/tuplex/tuplex at commit 5380aec8ab38017401610435511450cda9fd8b3b
# License: Apache-2.0
# Path: benchmarks/flights/runtuplex.py, lines 142-142
# Note: the lambda text is verbatim; the assignment 'udf = ' is added so the file is valid Python.

udf = lambda x: '{:02}:{:02}'.format(int(x / 100), x % 100) if x else None
