# codon-fuzz: entry=case_entry args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["A"], ["B"], ["C"], ["D"], ["B"]]
# real-udf: origin=literature id=tuplex_flights_clean_code requires= wrapped=1 ported=0
# Wrapper: Tuplex passes the row as a dict; case_entry builds it from the fields the UDF reads.
# Inputs: codes other than A-D return None (NULL); the rows avoid them.

# Source: Tuplex: Data Science in Python at Native Code Speed (Spiegelberg et al., SIGMOD 2021)
# URL: https://github.com/tuplex/tuplex/blob/5380aec8ab38017401610435511450cda9fd8b3b/benchmarks/flights/runtuplex.py#L145-L155
# Repository: https://github.com/tuplex/tuplex at commit 5380aec8ab38017401610435511450cda9fd8b3b
# License: Apache-2.0
# Path: benchmarks/flights/runtuplex.py, lines 145-155

def cleanCode(t):
    if t["CancellationCode"] == 'A':
        return 'carrier'
    elif t["CancellationCode"] == 'B':
        return 'weather'
    elif t["CancellationCode"] == 'C':
        return 'national air system'
    elif t["CancellationCode"] == 'D':
        return 'security'
    else:
        return None


def case_entry(cancellation_code):
    return cleanCode({"CancellationCode": cancellation_code})
