# codon-fuzz: entry=case_entry args=INT64,INT64 returns=BOOLEAN
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [[2019, 0], [1990, 1991], [2005, 2001], [2012, 2012], [1988, 2015], [2003, 0]]
# real-udf: origin=literature id=tuplex_flights_filter_defunct requires= wrapped=1 ported=0
# Wrapper: Tuplex passes the row as a dict; case_entry builds it from the fields the UDF reads.
# Inputs: AirlineYearDefunct 0 stands for a missing one (airline still flies).

# Source: Tuplex: Data Science in Python at Native Code Speed (Spiegelberg et al., SIGMOD 2021)
# URL: https://github.com/tuplex/tuplex/blob/5380aec8ab38017401610435511450cda9fd8b3b/benchmarks/flights/runtuplex.py#L252-L259
# Repository: https://github.com/tuplex/tuplex at commit 5380aec8ab38017401610435511450cda9fd8b3b
# License: Apache-2.0
# Path: benchmarks/flights/runtuplex.py, lines 252-259

def filterDefunctFlights(row):
    year = row['Year']
    airlineYearDefunct = row['AirlineYearDefunct']

    if airlineYearDefunct:
        return int(year) < int(airlineYearDefunct)
    else:
        return True


def case_entry(year, airline_year_defunct):
    return filterDefunctFlights({"Year": year, "AirlineYearDefunct": airline_year_defunct})
