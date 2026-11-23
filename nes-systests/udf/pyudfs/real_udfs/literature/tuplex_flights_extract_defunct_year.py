# codon-fuzz: entry=case_entry args=VARSIZED returns=INT64
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["Trans World Airways LLC (1960 - 2001)"], ["Continental Air Lines Inc. (1960 - 2012)"], ["Pan American World Airways (PAA) (1927 - 1991)"], ["US Airways Inc. (1997 - 2015)"], ["America West Airlines Inc. (1983 - 2007)"]]
# real-udf: origin=literature id=tuplex_flights_extract_defunct_year requires= wrapped=1 ported=0
# Wrapper: Tuplex passes the row as a dict; case_entry builds it from the fields the UDF reads.
# Inputs: airlines that still fly return None (NULL); the rows use defunct airlines.

# Source: Tuplex: Data Science in Python at Native Code Speed (Spiegelberg et al., SIGMOD 2021)
# URL: https://github.com/tuplex/tuplex/blob/5380aec8ab38017401610435511450cda9fd8b3b/benchmarks/flights/runtuplex.py#L189-L192
# Repository: https://github.com/tuplex/tuplex at commit 5380aec8ab38017401610435511450cda9fd8b3b
# License: Apache-2.0
# Path: benchmarks/flights/runtuplex.py, lines 189-192

def extractDefunctYear(t):
    x = t['Description']
    desc = x[x.rfind('-') + 1:x.rfind(')')].strip()
    return int(desc) if len(desc) > 0 else None


def case_entry(description):
    return extractDefunctYear({"Description": description})
