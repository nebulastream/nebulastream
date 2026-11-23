# codon-fuzz: entry=world_cups args=VARSIZED returns=INT64
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["Brazil"], ["Germany"], ["Italy"], ["Argentina"], ["Uruguay"], ["France"], ["England"], ["Spain"]]
# real-udf: origin=literature id=duckdb_world_cups requires= wrapped=0 ported=0
# Inputs: only countries in the dict; any other country returns None (NULL in DuckDB).

# Source: From Waddle to Flying: Quickly Expanding DuckDB's Functionality with Scalar Python UDFs (DuckDB blog, Holanda, 2023)
# URL: https://github.com/duckdb/duckdb-web/blob/7d751a272ab6fe1cb40c4d2f1d1a5c2daa41495b/_posts/2023-07-07-python-udf.md#L55-L69
# Repository: https://github.com/duckdb/duckdb-web at commit 7d751a272ab6fe1cb40c4d2f1d1a5c2daa41495b
# License: MIT
# Path: _posts/2023-07-07-python-udf.md, lines 55-69

# Dictionary that maps countries and world cups they won
world_cup_titles = {
    "Brazil": 5,
    "Germany": 4,
    "Italy": 4,
    "Argentina": 2,
    "Uruguay": 2,
    "France": 2,
    "England": 1,
    "Spain": 1
}

# Function that will be registered as an UDF, simply does a lookup in the python dictionary
def world_cups(x):
     return world_cup_titles.get(x)
