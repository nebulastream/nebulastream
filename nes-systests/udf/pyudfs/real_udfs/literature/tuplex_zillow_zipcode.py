# codon-fuzz: entry=case_entry args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["2134"], ["2139"], ["10027"], ["98101"], ["501"], ["02116"]]
# real-udf: origin=literature id=tuplex_zillow_zipcode requires= wrapped=1 ported=0
# Wrapper: Tuplex passes the row as a dict; case_entry builds it from the fields the UDF reads.

# Source: Tuplex: Data Science in Python at Native Code Speed (Spiegelberg et al., SIGMOD 2021)
# URL: https://github.com/tuplex/tuplex/blob/5380aec8ab38017401610435511450cda9fd8b3b/benchmarks/zillow/Z1/runtuplex.py#L196
# Repository: https://github.com/tuplex/tuplex at commit 5380aec8ab38017401610435511450cda9fd8b3b
# License: Apache-2.0
# Path: benchmarks/zillow/Z1/runtuplex.py, lines 196-196
# Note: the lambda text is verbatim; the assignment 'udf = ' is added so the file is valid Python.

udf = lambda x: '%05d' % int(x['postal_code'])


def case_entry(postal_code):
    return udf({"postal_code": postal_code})
