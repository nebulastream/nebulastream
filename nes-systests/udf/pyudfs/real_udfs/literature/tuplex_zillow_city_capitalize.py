# codon-fuzz: entry=udf args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["boston"], ["CAMBRIDGE"], ["somerville"], ["new york"], ["sAN fRANCISCO"], ["X"]]
# real-udf: origin=literature id=tuplex_zillow_city_capitalize requires= wrapped=0 ported=0

# Source: Tuplex: Data Science in Python at Native Code Speed (Spiegelberg et al., SIGMOD 2021)
# URL: https://github.com/tuplex/tuplex/blob/5380aec8ab38017401610435511450cda9fd8b3b/benchmarks/zillow/Z1/runtuplex.py#L197
# Repository: https://github.com/tuplex/tuplex at commit 5380aec8ab38017401610435511450cda9fd8b3b
# License: Apache-2.0
# Path: benchmarks/zillow/Z1/runtuplex.py, lines 197-197
# Note: the lambda text is verbatim; the assignment 'udf = ' is added so the file is valid Python.

udf = lambda x: x[0].upper() + x[1:].lower()
