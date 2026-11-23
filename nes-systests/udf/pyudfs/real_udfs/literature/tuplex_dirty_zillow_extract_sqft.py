# codon-fuzz: entry=case_entry args=VARSIZED returns=INT64
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["3 bds , 2 ba , 1,560 sqft"], ["4 bds , 3 ba , 2,000 sqft"], ["2 bds , 1 ba , 850 sqft"], ["5 bds , 4 ba , 3,950 sqft"], ["1 bd , 1 ba , 640 sqft"], ["6 bds , 5 ba , 4,210 sqft"], ["Studio , 1 ba , 500 sqft"]]
# real-udf: origin=literature id=tuplex_dirty_zillow_extract_sqft requires= wrapped=1 ported=0
# Wrapper: Tuplex passes the row as a dict; case_entry builds it from the fields the UDF reads.
# Wrapper: The benchmark script imports re at its top; the case adds the import before the UDF code.
# Inputs: facts without a numeric area return None (NULL); the rows avoid them.
import re

# Source: Tuplex: Data Science in Python at Native Code Speed (Spiegelberg et al., SIGMOD 2021)
# URL: https://github.com/tuplex/tuplex/blob/5380aec8ab38017401610435511450cda9fd8b3b/benchmarks/dirty_zillow/runtuplex.py#L315-L334
# Repository: https://github.com/tuplex/tuplex at commit 5380aec8ab38017401610435511450cda9fd8b3b
# License: Apache-2.0
# Path: benchmarks/dirty_zillow/runtuplex.py, lines 315-334

def extractSqft(x):
    val = x['facts and features']
    max_idx = val.find(' sqft')
    if max_idx < 0:
        max_idx = len(val)
    s = val[:max_idx]

    split_idx = s.rfind('ba ,')
    if split_idx < 0:
        split_idx = 0
    else:
        split_idx += 5
    r = s[split_idx:]
    r = r.replace(',', '')

    m = re.search(r'^\d+$', r)
    if m:
        return int(r)
    else:
        return None


def case_entry(facts_and_features):
    return extractSqft({"facts and features": facts_and_features})
