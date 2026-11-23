# codon-fuzz: entry=case_entry args=VARSIZED returns=INT64
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["3 bds , 2 ba , 1,560 sqft"], ["4 bds , 3 ba , 2,000 sqft"], ["2 bds , 1 ba , 850 sqft"], ["5 bds , 4 ba , 3,950 sqft"], ["Studio , 1 ba , 500 sqft"], ["1 bd , 1 ba , 640 sqft"]]
# real-udf: origin=literature id=tuplex_dirty_zillow_extract_bd requires= wrapped=1 ported=0
# Wrapper: Tuplex passes the row as a dict; case_entry builds it from the fields the UDF reads.
# Wrapper: The benchmark script imports re at its top; the case adds the import before the UDF code.
import re

# Source: Tuplex: Data Science in Python at Native Code Speed (Spiegelberg et al., SIGMOD 2021)
# URL: https://github.com/tuplex/tuplex/blob/5380aec8ab38017401610435511450cda9fd8b3b/benchmarks/dirty_zillow/runtuplex.py#L268-L290
# Repository: https://github.com/tuplex/tuplex at commit 5380aec8ab38017401610435511450cda9fd8b3b
# License: Apache-2.0
# Path: benchmarks/dirty_zillow/runtuplex.py, lines 268-290

def extractBd(x):
    val = x['facts and features']
    max_idx = val.find(' bd')
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

    # regex check whether we can cast
    m = re.search(r'^\d+$', r)
    if m:
        return int(r)
    elif 'studio' in val.lower():
        return 1
    else:
        return None


def case_entry(facts_and_features):
    return extractBd({"facts and features": facts_and_features})
