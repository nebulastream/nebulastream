# codon-fuzz: entry=case_entry args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["New York, NY"], ["Dallas/Fort Worth, TX"], ["Chicago, IL"], ["Washington, DC"], ["Kona, HI"], ["St. Louis, MO"], ["Sun Valley/Hailey/Ketchum, ID"]]
# real-udf: origin=literature id=tuplex_flights_origin_state requires= wrapped=1 ported=0
# Wrapper: Tuplex passes the row as a dict; case_entry builds it from the fields the UDF reads.

# Source: Tuplex: Data Science in Python at Native Code Speed (Spiegelberg et al., SIGMOD 2021)
# URL: https://github.com/tuplex/tuplex/blob/5380aec8ab38017401610435511450cda9fd8b3b/benchmarks/flights/runtuplex.py#L137
# Repository: https://github.com/tuplex/tuplex at commit 5380aec8ab38017401610435511450cda9fd8b3b
# License: Apache-2.0
# Path: benchmarks/flights/runtuplex.py, lines 137-137
# Note: the lambda text is verbatim; the assignment 'udf = ' is added so the file is valid Python.

udf = lambda x: x['OriginCityName'][x['OriginCityName'].rfind(',')+1:].strip()


def case_entry(origin_city_name):
    return udf({"OriginCityName": origin_city_name})
