# codon-fuzz: entry=case_entry args=FLOAT64,VARSIZED,VARSIZED returns=FLOAT64
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [[145.0, "", ""], [87.0, "", ""], [210.0, "1.00", "263.00"], [98.0, "0.00", ""], [312.0, "1.00", "401.00"], [65.0, "", ""]]
# real-udf: origin=literature id=tuplex_flights_fill_in_times requires= wrapped=1 ported=0
# Wrapper: Tuplex passes the row as a dict; case_entry builds it from the fields the UDF reads.
# Inputs: an empty DivReachedDest stands for a missing one.

# Source: Tuplex: Data Science in Python at Native Code Speed (Spiegelberg et al., SIGMOD 2021)
# URL: https://github.com/tuplex/tuplex/blob/5380aec8ab38017401610435511450cda9fd8b3b/benchmarks/flights/runtuplex.py#L168-L176
# Repository: https://github.com/tuplex/tuplex at commit 5380aec8ab38017401610435511450cda9fd8b3b
# License: Apache-2.0
# Path: benchmarks/flights/runtuplex.py, lines 168-176

def fillInTimesUDF(row):
    ACTUAL_ELAPSED_TIME = row['ActualElapsedTime']
    if row['DivReachedDest']:
        if float(row['DivReachedDest']) > 0:
            return float(row['DivActualElapsedTime'])
        else:
            return ACTUAL_ELAPSED_TIME
    else:
        return ACTUAL_ELAPSED_TIME


def case_entry(actual_elapsed_time, div_reached_dest, div_actual_elapsed_time):
    return fillInTimesUDF({"ActualElapsedTime": actual_elapsed_time, "DivReachedDest": div_reached_dest,
                           "DivActualElapsedTime": div_actual_elapsed_time})
