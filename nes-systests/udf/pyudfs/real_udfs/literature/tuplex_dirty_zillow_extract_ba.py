# codon-fuzz: entry=case_entry args=VARSIZED returns=FLOAT64
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["3 bds , 2 ba , 1,560 sqft"], ["4 bds , 3 ba , 2,000 sqft"], ["2 bds , 1 ba , 850 sqft"], ["3 bds , 2.5 ba , 1,850 sqft"], ["2 bds , 1.75 ba , 1,100 sqft"], ["4 ba"]]
# real-udf: origin=literature id=tuplex_dirty_zillow_extract_ba requires= wrapped=1 ported=0
# Wrapper: Tuplex passes the row as a dict; case_entry builds it from the fields the UDF reads.
# Wrapper: The benchmark script imports re and math at its top; the case adds the import before the UDF code.
# Inputs: facts without a numeric bathroom count return None (NULL); the rows avoid them.
import math
import re

# Source: Tuplex: Data Science in Python at Native Code Speed (Spiegelberg et al., SIGMOD 2021)
# URL: https://github.com/tuplex/tuplex/blob/5380aec8ab38017401610435511450cda9fd8b3b/benchmarks/dirty_zillow/runtuplex.py#L292-L313
# Repository: https://github.com/tuplex/tuplex at commit 5380aec8ab38017401610435511450cda9fd8b3b
# License: Apache-2.0
# Path: benchmarks/dirty_zillow/runtuplex.py, lines 292-313

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

    # check if valid float string by using regex matching
    m = re.search(r'^\d+.?\d*$', r)
    if m:
        ba = math.ceil(2.0 * float(r)) / 2.0
        return ba
    else:
        return None


def case_entry(facts_and_features):
    return extractBa({"facts and features": facts_and_features})
