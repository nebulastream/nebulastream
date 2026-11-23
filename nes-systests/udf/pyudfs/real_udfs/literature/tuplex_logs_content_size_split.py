# codon-fuzz: entry=udf args=VARSIZED returns=INT64
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["6245"], ["3985"], ["0"], ["-"], ["40310"], ["110"]]
# real-udf: origin=literature id=tuplex_logs_content_size_split requires= wrapped=0 ported=0

# Source: Tuplex: Data Science in Python at Native Code Speed (Spiegelberg et al., SIGMOD 2021)
# URL: https://github.com/tuplex/tuplex/blob/5380aec8ab38017401610435511450cda9fd8b3b/benchmarks/logs/runtuplex.py#L272
# Repository: https://github.com/tuplex/tuplex at commit 5380aec8ab38017401610435511450cda9fd8b3b
# License: Apache-2.0
# Path: benchmarks/logs/runtuplex.py, lines 272-272
# Note: the lambda text is verbatim; the assignment 'udf = ' is added so the file is valid Python.

udf = lambda x: 0 if x == '-' else int(x)
