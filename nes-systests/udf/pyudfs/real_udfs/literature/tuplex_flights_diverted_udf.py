# codon-fuzz: entry=case_entry args=BOOLEAN,VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [[true, ""], [false, "A"], [false, "B"], [false, ""], [false, "D"], [true, "C"]]
# real-udf: origin=literature id=tuplex_flights_diverted_udf requires= wrapped=1 ported=0
# Wrapper: Tuplex passes the row as a dict; case_entry builds it from the fields the UDF reads.
# Inputs: an empty CancellationCode stands for a missing one.

# Source: Tuplex: Data Science in Python at Native Code Speed (Spiegelberg et al., SIGMOD 2021)
# URL: https://github.com/tuplex/tuplex/blob/5380aec8ab38017401610435511450cda9fd8b3b/benchmarks/flights/runtuplex.py#L157-L166
# Repository: https://github.com/tuplex/tuplex at commit 5380aec8ab38017401610435511450cda9fd8b3b
# License: Apache-2.0
# Path: benchmarks/flights/runtuplex.py, lines 157-166

def divertedUDF(row):
    diverted = row['Diverted']
    ccode = row['CancellationCode']
    if diverted:
        return 'diverted'
    else:
        if ccode:
            return ccode
        else:
            return 'None'


def case_entry(diverted, cancellation_code):
    return divertedUDF({"Diverted": diverted, "CancellationCode": cancellation_code})
