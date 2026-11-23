# codon-fuzz: entry=case_entry args=VARSIZED returns=INT64
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["3 bds , 2 ba , 1,560 sqft"], ["4 bds , 3 ba , 2,000 sqft"], ["1 bd , 1 ba"], ["10 bds , 6 ba , 7,800 sqft"], ["2 bds , 1 ba , 850 sqft"], ["5 bds"]]
# real-udf: origin=literature id=tuplex_zillow_extract_bd requires= wrapped=1 ported=0
# Wrapper: Tuplex passes the row as a dict; case_entry builds it from the one field the UDF reads.
# Source: Tuplex: Data Science in Python at Native Code Speed (Spiegelberg et al., SIGMOD 2021)
# URL: https://github.com/tuplex/tuplex/blob/5380aec8ab38017401610435511450cda9fd8b3b/benchmarks/zillow/Z1/runtuplex.py#L12-L26
# Repository: https://github.com/tuplex/tuplex at commit 5380aec8ab38017401610435511450cda9fd8b3b
# License: Apache-2.0
# Path: benchmarks/zillow/Z1/runtuplex.py, lines 12-26

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
    return int(r)


def case_entry(facts_and_features):
    return extractBd({"facts and features": facts_and_features})
