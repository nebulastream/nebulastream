# codon-fuzz: entry=udf args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["American Airlines Inc."], ["Southwest Airlines Co."], ["Trans World Airways LLC"], ["JetBlue Airways"], ["Endeavor Air Inc."], ["Atlantic Coast Airlines"]]
# real-udf: origin=literature id=tuplex_flights_strip_company_suffix requires= wrapped=0 ported=0

# Source: Tuplex: Data Science in Python at Native Code Speed (Spiegelberg et al., SIGMOD 2021)
# URL: https://github.com/tuplex/tuplex/blob/5380aec8ab38017401610435511450cda9fd8b3b/benchmarks/flights/runtuplex.py#L233-L235
# Repository: https://github.com/tuplex/tuplex at commit 5380aec8ab38017401610435511450cda9fd8b3b
# License: Apache-2.0
# Path: benchmarks/flights/runtuplex.py, lines 233-235

udf = lambda s: s.replace('Inc.', '') \
                          .replace('LLC', '') \
                          .replace('Co.', '').strip()
