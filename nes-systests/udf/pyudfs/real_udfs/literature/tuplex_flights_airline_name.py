# codon-fuzz: entry=case_entry args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["American Airlines Inc. (1960 - )"], ["Delta Air Lines Inc. (1960 - )"], ["Southwest Airlines Co. (1979 - )"], ["Trans World Airways LLC (1960 - 2001)"], ["Continental Air Lines Inc. (1960 - 2012)"], ["JetBlue Airways (2000 - )"], ["Pan American World Airways (PAA) (1927 - 1991)"]]
# real-udf: origin=literature id=tuplex_flights_airline_name requires= wrapped=1 ported=0
# Wrapper: Tuplex passes the row as a dict; case_entry builds it from the fields the UDF reads.

# Source: Tuplex: Data Science in Python at Native Code Speed (Spiegelberg et al., SIGMOD 2021)
# URL: https://github.com/tuplex/tuplex/blob/5380aec8ab38017401610435511450cda9fd8b3b/benchmarks/flights/runtuplex.py#L195
# Repository: https://github.com/tuplex/tuplex at commit 5380aec8ab38017401610435511450cda9fd8b3b
# License: Apache-2.0
# Path: benchmarks/flights/runtuplex.py, lines 195-195
# Note: the lambda text is verbatim; the assignment 'udf = ' is added so the file is valid Python.

udf = lambda x: x['Description'][:x['Description'].rfind('(')].strip()


def case_entry(description):
    return udf({"Description": description})
