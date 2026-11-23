# codon-fuzz: entry=case_entry args=VARSIZED returns=FLOAT64
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["3 bds , 2 ba , 1,560 sqft"], ["4 bds , 3 ba , 2,000 sqft"], ["2 bds , 1 ba , 850 sqft"], ["5 bds , 4 ba , 3,950 sqft"], ["3 bds , 2.5 ba , 1,850 sqft"], ["2 bds , 1.75 ba , 1,100 sqft"]]
# real-udf: origin=literature id=tuplex_zillow_z2_extract_ba requires= wrapped=1 ported=0
# Wrapper: Tuplex passes the row as a dict; case_entry builds it from the fields the UDF reads.
# Wrapper: The benchmark script imports math at its top; the case adds the import before the UDF code.
import math

# Source: Tuplex: Data Science in Python at Native Code Speed (Spiegelberg et al., SIGMOD 2021)
# URL: https://github.com/tuplex/tuplex/blob/5380aec8ab38017401610435511450cda9fd8b3b/benchmarks/zillow/Z2/runtuplex.py#L31-L47
# Repository: https://github.com/tuplex/tuplex at commit 5380aec8ab38017401610435511450cda9fd8b3b
# License: Apache-2.0
# Path: benchmarks/zillow/Z2/runtuplex.py, lines 31-47

def extractBa(x):
    val = x['facts and features']
    max_idx = val.find(' ba')
    if max_idx < 0:
        max_idx = len(val)
    s = val[:max_idx]

    # find comma before
    split_idx = s.rfind(',')
    if split_idx < 0:
        split_idx = 0
    else:
        split_idx += 2
    r = s[split_idx:]

    ba = math.ceil(2.0 * float(r)) / 2.0
    return ba


def case_entry(facts_and_features):
    return extractBa({"facts and features": facts_and_features})
